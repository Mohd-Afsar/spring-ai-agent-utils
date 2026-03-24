package org.springaicommunity.nova;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Locale;

import javax.sql.DataSource;

import java.util.List;
import java.util.Optional;

import org.springaicommunity.agent.tools.DatabaseTools;
import org.springaicommunity.nova.graph.GraphTopologyService;
import org.springaicommunity.nova.tools.AlarmAnalystTool;
import org.springaicommunity.nova.tools.KPIAnalystTool;
import org.springaicommunity.nova.tools.NetworkIntelligenceTool;
import org.springaicommunity.nova.tools.NetworkTopologyRcaTool;
import org.springaicommunity.nova.tools.PmDataFetchTool;
import org.springaicommunity.nova.tools.ReportFormatterTool;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.client.advisor.ToolCallAdvisor;
import org.springframework.ai.chat.memory.MessageWindowChatMemory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.io.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NOVA — Network Operations Virtual Agent
 *
 * <p>A multi-agent NOC system with NOVA as the orchestrator and four specialist
 * sub-agents for alarm analysis, KPI analysis, network intelligence, and
 * report formatting. All agents query the live railtel MySQL database.
 *
 * <p>Architecture:
 * <pre>
 *   User
 *    │
 *   NOVA (NOC Manager orchestrator)
 *    ├── DatabaseTools     → raw SQL queries against railtel DB
 *    ├── AlarmAnalyst      → LLM specialist: alarm patterns, storm risk, vendor sigs
 *    ├── KPIAnalyst        → LLM specialist: traffic, utilization, DDoS detection
 *    ├── NetworkIntelligence → LLM specialist: topology, root cause, propagation
 *    └── ReportFormatter   → LLM specialist: final structured report generation
 * </pre>
 *
 * <p>The web server (port 8080) runs concurrently and exposes PM data REST APIs
 * for programmatic agent consumption.
 *
 * @author Spring AI Community
 */
@SpringBootApplication
public class NovaApplication {

	private static final Logger log = LoggerFactory.getLogger(NovaApplication.class);

	/** Max retries on HTTP 429 before giving up. */
	private static final int MAX_RETRIES = 3;

	/**
	 * Cap on wait between retries. PM analysis involves two LLM calls
	 * (PmDataFetch → PmAnalyst) so rate-limit windows can be tight on Groq.
	 * A 30s cap gives the token quota time to reset between retries.
	 */
	private static final int MAX_WAIT_SECONDS = 30;
	private static final int MAX_ALARM_DATA_CHARS_FOR_LLM = 100000;
	private static final String DEFAULT_ALARM_FILTER_SQL = """
			SELECT
			    ALARM_EXTERNAL_ID,
			    ALARM_CODE,
			    ALARM_NAME,
			    OPEN_TIME,
			    CHANGE_TIME,
			    LAST_PROCESSING_TIME,
			    FIRST_RECEPTION_TIME,
			    REPORTING_TIME,
			    SEVERITY,
			    ACTUAL_SEVERITY,
			    ALARM_STATUS,
			    CLASSIFICATION,
			    ENTITY_ID,
			    ENTITY_NAME,
			    ENTITY_TYPE,
			    SUBENTITY,
			    PARENT_ENTITY_ID,
			    GEOGRAPHY_L1_NAME,
			    GEOGRAPHY_L2_NAME,
			    GEOGRAPHY_L3_NAME,
			    VENDOR,
			    DOMAIN,
			    TECHNOLOGY,
			    PROBABLE_CAUSE,
			    DESCRIPTION,
			    EVENT_TYPE,
			    SERVICE_AFFECTED,
			    INCIDENT_ID,
			    CORRELATION_FLAG,
			    ALARM_GROUP
			FROM ALARM
			WHERE ALARM_STATUS IN ('OPEN','REOPEN')
			  AND SEVERITY IN ('CRITICAL','MAJOR','MINOR')
			""";
	private static final String DEFAULT_ALARM_HIERARCHY_SEED_SQL = """
			SELECT DISTINCT ENTITY_NAME
			FROM ALARM
			WHERE ALARM_STATUS IN ('OPEN','REOPEN')
			  AND SEVERITY IN ('CRITICAL','MAJOR')
			""";

	public static void main(String[] args) {
		SpringApplication.run(NovaApplication.class, args);
	}

	@Bean
	CommandLineRunner commandLineRunner(ChatClient.Builder chatClientBuilder,
			@Value("classpath:/prompt/NOVA_SYSTEM_PROMPT.md") Resource systemPrompt,
			@Value("${agent.model:openai/gpt-oss-120b}") String agentModel,
			@Value("${agent.model.knowledge.cutoff:Unknown}") String agentModelKnowledgeCutoff,
			@Value("${pm.api.base-url:http://localhost:8080}") String pmApiBaseUrl,
			@Value("${nova.topology.max-alarming-nodes:120}") int novaTopologyMaxAlarmingNodes,
			@Value("${nova.topology.max-topology-chars-for-network-intelligence:100000}") int novaTopologyMaxCharsForNi,
			@Value("${nova.topology.max-children-per-node:30}") int novaTopologyMaxChildrenPerNode,
			@Value("${nova.topology.max-link-neighbors-per-node:25}") int novaTopologyMaxLinkNeighbors,
			@Autowired DataSource dataSource,
			org.springaicommunity.nova.pm.analytics.PmAnalyticsEngine analyticsEngine,
			Optional<GraphTopologyService> graphTopologyService) {

	return args -> {
		System.out.println("--------------------------------");
		System.out.println("agentModel: " + agentModel);
		System.out.println("pmApiBaseUrl: " + pmApiBaseUrl);
		System.out.println("dataSource: " + dataSource);
		System.out.println("analyticsEngine: " + analyticsEngine);
		System.out.println("--------------------------------\n\n\n");

		// Clone the builder BEFORE adding any NOVA-specific tools/system prompt.
		// Sub-agents must get a clean, tool-free builder so their LLM cannot call
		// DbQuery, ReportFormatter, or any other NOVA tool — it must only generate text.
		ChatClient.Builder subAgentBuilder = chatClientBuilder.clone();
		GraphTopologyService graphServiceOrNull = graphTopologyService.orElse(null);
		DatabaseTools databaseTools = DatabaseTools.builder(dataSource).maxRows(500).build();
		AlarmAnalystTool alarmAnalystTool = AlarmAnalystTool.builder(subAgentBuilder.clone())
				.build();
		KPIAnalystTool kpiAnalystTool = KPIAnalystTool.builder(subAgentBuilder.clone()).build();
		NetworkIntelligenceTool networkIntelligenceTool = NetworkIntelligenceTool.builder(subAgentBuilder.clone()).build();
		PmDataFetchTool pmDataFetchTool = PmDataFetchTool.builder()
				.pmApiBaseUrl(pmApiBaseUrl)
				.analyticsEngine(analyticsEngine)
				.chatClientBuilder(subAgentBuilder.clone())
				.dataSource(dataSource)
				.build();
		ReportFormatterTool reportFormatterTool = ReportFormatterTool.builder(subAgentBuilder.clone())
				.graphTopologyService(null)
				.build();
		NetworkTopologyRcaTool networkTopologyRcaTool = (graphServiceOrNull != null)
				? NetworkTopologyRcaTool.of(graphServiceOrNull, subAgentBuilder.clone(), novaTopologyMaxAlarmingNodes,
						novaTopologyMaxChildrenPerNode, novaTopologyMaxLinkNeighbors, novaTopologyMaxCharsForNi, dataSource)
				: null;

		ChatClient.Builder novaBuilder = chatClientBuilder
			.defaultSystem(p -> p.text(systemPrompt)
				.param("AGENT_MODEL", agentModel)
				.param("AGENT_MODEL_KNOWLEDGE_CUTOFF", agentModelKnowledgeCutoff))

		// Layer 1: Direct database access (NOVA queries the DB herself)
		.defaultTools(databaseTools)

		// Layer 2: Specialist sub-agents — built with the clean sub-agent builder
		// so they cannot call each other's tools or NOVA's DB tools.
		// PmDataFetchTool combines fetch + analytics + report generation in one step,
		// eliminating the two-tool chain that caused XML-format call errors.
		.defaultTools(
			alarmAnalystTool,
			kpiAnalystTool,
			networkIntelligenceTool,
			pmDataFetchTool,
			reportFormatterTool);
			// JanusGraph integration removed from NOVA runtime flow.

		ChatClient nova = novaBuilder

				.defaultAdvisors(
					ToolCallAdvisor.builder()
						.conversationHistoryEnabled(false)
						.build(),
					MessageChatMemoryAdvisor.builder(MessageWindowChatMemory.builder().maxMessages(300).build())
						.order(Ordered.HIGHEST_PRECEDENCE + 1000)
						.build())

				.build();
			log.info("[NOVA] JanusGraph integration: disabled");

			System.out.println();
			System.out.println("╔══════════════════════════════════════════════════════╗");
			System.out.println("║  NOVA — Network Operations Virtual Agent             ║");
			System.out.println("╠══════════════════════════════════════════════════════╣");
			System.out.println("║  Type 'exit' to quit                                 ║");
			System.out.println("╚══════════════════════════════════════════════════════╝");
			System.out.println();

			try (Scanner scanner = new Scanner(System.in)) {
				while (true) {
					System.out.print("\n> YOU: ");
					String input = scanner.nextLine().trim();
					if (input.isBlank()) continue;
					if (isLogLine(input)) continue;
					if ("exit".equalsIgnoreCase(input)) {
						System.out.println("NOVA: Shift handed over. Signing off.");
						System.exit(0);
					}
					if (isAlarmAnalysisRequest(input)) {
						log.info("[NOVA] Running deterministic alarm orchestration for input='{}'", input);
						String response = runDeterministicAlarmFlow(input, databaseTools, alarmAnalystTool, networkTopologyRcaTool);
						System.out.println("\nNOVA: " + response);
						continue;
					}
					if (isAlarmHierarchyTopologyRequest(input)) {
						log.info("[NOVA] Running deterministic alarm hierarchy topology flow for input='{}'", input);
						String response = runDeterministicAlarmHierarchyFlow(input, databaseTools, networkTopologyRcaTool);
						System.out.println("\nNOVA: " + response);
						continue;
					}
					if (isPmKpiIntent(input)) {
						log.info("[NOVA][Routing] Running deterministic PM/KPI orchestration for input='{}'", input);
						String response = runDeterministicPmFlow(input, pmDataFetchTool);
						System.out.println("\nNOVA: " + response);
						continue;
					}
					String response = callWithRetry(nova, input);
					System.out.println("\nNOVA: " + response);
				}
			}
		};
	}

	/**
	 * Calls NOVA with a live elapsed-time indicator and retries on HTTP 429.
	 *
	 * <p>The progress ticker runs on a daemon thread so it never blocks shutdown.
	 * On 429 rate-limit responses, we back off up to {@link #MAX_WAIT_SECONDS}
	 * and retry up to {@link #MAX_RETRIES} times before giving up.
	 */
	private static String callWithRetry(ChatClient nova, String input) throws InterruptedException {
		int waitSec = 2;
		for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
			AgentConsole.beginToolTracking();
			long startMs = System.currentTimeMillis();
			AtomicBoolean done = new AtomicBoolean(false);

			Thread progress = new Thread(() -> {
				try {
					while (!done.get()) {
						long elapsed = (System.currentTimeMillis() - startMs) / 1000;
						String tool = AgentConsole.currentTool.get();
						String toolTag = tool.isBlank() ? "" : " [→ " + tool + "]";
						String line = String.format("\rWorking... %ds%s  ", elapsed, toolTag);
						// Pad to 60 chars so leftover chars from longer lines are erased
						System.out.printf("%-60s", line);
						System.out.flush();
						Thread.sleep(1000);
					}
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				System.out.printf("%-60s\r", "");
				System.out.flush();
			});
			progress.setDaemon(true);
			progress.start();

			try {
				log.info("----------------INPUT START-------------");
				log.info("INPUT GIVEN: {}",input);
				log.info("----------------INPUT END-------------");

				String result = nova.prompt(input).call().content();
				
				done.set(true);
				progress.join();
				List<String> toolsUsed = AgentConsole.endToolTracking();
				boolean janusGraphUsed = AgentConsole.isJanusGraphEvidenceCalled();
				log.info("[NOVA] Tools used this turn: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);
				return result;
			}
			catch (Exception e) {
				done.set(true);
				progress.join();
				List<String> toolsUsed = AgentConsole.endToolTracking();
				boolean janusGraphUsed = AgentConsole.isJanusGraphEvidenceCalled();
				log.info("[NOVA] Tools used this turn before failure: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);

				String msg = e.getMessage() != null ? e.getMessage() : "";
				boolean isRateLimit = msg.contains("429") || msg.contains("rate_limit_exceeded")
						|| msg.contains("Rate limit");
				// 503 = model over capacity — treat as retriable, but with longer wait
				boolean isOverCapacity = msg.contains("503") || msg.contains("over capacity")
						|| msg.contains("TransientAiException");

				if ((isRateLimit || isOverCapacity) && attempt < MAX_RETRIES) {
					// Over-capacity needs a longer pause than a simple rate-limit
					int wait = isOverCapacity ? Math.max(waitSec * 2, 30) : Math.min(waitSec, MAX_WAIT_SECONDS);
					String reason = isOverCapacity ? "Model over capacity" : "Rate limited";
					System.out.printf("[%s — waiting %ds before retry %d/%d]%n",
							reason, wait, attempt, MAX_RETRIES);
					Thread.sleep(wait * 1000L);
					waitSec = Math.min(waitSec * 2, MAX_WAIT_SECONDS);
				}
				else if (isRateLimit || isOverCapacity) {
					return "[" + (isOverCapacity ? "Model over capacity" : "Rate limit")
							+ " not resolved after " + MAX_RETRIES + " retries. "
							+ "Wait ~1 minute then try a more specific query.]";
				}
				else {
					return "[Error: " + msg + "]";
				}
			}
		}

		return "[Request could not complete — please try again.]";
	}

	/**
	 * Returns true for lines that look like Spring Boot / Java log output so they
	 * are silently skipped when the user accidentally pastes terminal output into chat.
	 * Pattern: "2026-03-19T14:00:00.000+05:30  INFO  ..."
	 */
	private static boolean isLogLine(String line) {
		// Timestamp prefix: "20YY-MM-DDT..."
		return line.length() > 10 && line.charAt(4) == '-' && line.charAt(7) == '-'
				&& line.charAt(10) == 'T' && Character.isDigit(line.charAt(0));
	}

	private static boolean isAlarmAnalysisRequest(String input) {
		if (input == null) return false;
		String lower = input.toLowerCase();
		return lower.contains("alarm analysis")
				|| lower.contains("alarm report")
				|| lower.contains("overall alarms")
				|| lower.contains("critical alarms")
				|| lower.contains("root cause")
				|| lower.contains("rca");
	}

	private static boolean isAlarmHierarchyTopologyRequest(String input) {
		if (input == null || input.isBlank()) return false;
		String lower = input.toLowerCase(Locale.ROOT);
		boolean asksTopology = lower.contains("hierarchy")
				|| lower.contains("topology")
				|| lower.contains("connected");
		boolean asksLinks = lower.contains("interface")
				|| lower.contains("interfaces")
				|| lower.contains("link")
				|| lower.contains("links");
		boolean alarmScoped = lower.contains("alarm");
		return alarmScoped && asksTopology && asksLinks;
	}

	private static String runDeterministicAlarmFlow(String userInput,
			DatabaseTools databaseTools,
			AlarmAnalystTool alarmAnalystTool,
			NetworkTopologyRcaTool networkTopologyRcaTool) {
		AgentConsole.beginToolTracking();
		try {
			String sql = buildAlarmFetchSql();
			String rawAlarmData = databaseTools.executeQuery(sql);
			String scope = "overall active alarms";
			String investigationContext = scope + " | userRequest='" + userInput + "'";
			if (networkTopologyRcaTool == null) {
				List<String> toolsUsed = AgentConsole.endToolTracking();
				log.info("[NOVA] Tools used this turn: {} | JanusGraph topology RCA invoked: false", toolsUsed);
				return "[Mandatory topology step failed: JanusGraph topology service is unavailable.]";
			}
			String hierarchySeedSql = buildAlarmHierarchySeedSql();
			String hierarchySeedData = databaseTools.executeQuery(hierarchySeedSql);
			String topologyEvidence = networkTopologyRcaTool.analyzeTopologyForAlarmMarkdown(hierarchySeedData, investigationContext, false);
			String alarmDataForLlm = shrinkAlarmDataForLlm(rawAlarmData, MAX_ALARM_DATA_CHARS_FOR_LLM);
			if (rawAlarmData != null && alarmDataForLlm.length() < rawAlarmData.length()) {
				log.warn("[NOVA] Truncated alarm data for AlarmAnalyst. originalChars={} sentChars={}",
						rawAlarmData.length(), alarmDataForLlm.length());
			}
			String alarmAnalysis = alarmAnalystTool.analyzeAlarms(alarmDataForLlm, investigationContext,
					topologyEvidence);

			List<String> toolsUsed = AgentConsole.endToolTracking();
			boolean janusGraphUsed = AgentConsole.isJanusGraphEvidenceCalled();
			log.info("[NOVA] Tools used this turn: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);
			return alarmAnalysis;
		}
		catch (Exception e) {
			List<String> toolsUsed = AgentConsole.endToolTracking();
			boolean janusGraphUsed = AgentConsole.isJanusGraphEvidenceCalled();
			log.info("[NOVA] Tools used this turn before failure: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);
			return "[Error during deterministic alarm orchestration: " + e.getMessage() + "]";
		}
	}

	private static String runDeterministicAlarmHierarchyFlow(String userInput,
			DatabaseTools databaseTools,
			NetworkTopologyRcaTool networkTopologyRcaTool) {
		AgentConsole.beginToolTracking();
		try {
			if (networkTopologyRcaTool == null) {
				List<String> toolsUsed = AgentConsole.endToolTracking();
				log.info("[NOVA] Tools used this turn: {} | JanusGraph topology RCA invoked: false", toolsUsed);
				return "JanusGraph topology service is unavailable. Enable JanusGraph to fetch hierarchy, interfaces, and links.";
			}
			String seedSql = buildAlarmHierarchySeedSql();
			String alarmSeedData = databaseTools.executeQuery(seedSql);
			String context = "alarm-hierarchy-topology | userRequest='" + userInput + "'";
			String topology = networkTopologyRcaTool.analyzeTopologyForAlarmMarkdown(alarmSeedData, context, false);
			List<String> toolsUsed = AgentConsole.endToolTracking();
			boolean janusGraphUsed = AgentConsole.isJanusGraphEvidenceCalled();
			log.info("[NOVA] Tools used this turn: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);
			return topology;
		}
		catch (Exception e) {
			List<String> toolsUsed = AgentConsole.endToolTracking();
			boolean janusGraphUsed = AgentConsole.isJanusGraphEvidenceCalled();
			log.info("[NOVA] Tools used this turn before failure: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);
			return "[Error during deterministic alarm hierarchy topology flow: " + e.getMessage() + "]";
		}
	}

	private static String runDeterministicPmFlow(String userInput, PmDataFetchTool pmDataFetchTool) {
		AgentConsole.beginToolTracking();
		try {
			String nodeName = extractFirstIpv4(userInput);
			log.info("[NOVA][Routing] PM deterministic request -> tool=PmAnalysis nodeName='{}' userQuery='{}'",
					nodeName == null ? "" : nodeName, userInput);
			String response = pmDataFetchTool.fetchEnrichedPmData(
					null, // domain -> tool default TRANSPORT
					null, // vendor -> tool default JUNIPER
					null, // technology -> tool default COMMON
					null, // dataLevel -> tool default ROUTER_COMMON_Router
					nodeName, // optional node filter (first IPv4 in query)
					null, // granularity -> default HOURLY
					null, // from -> configured fixed default
					null, // to -> configured fixed default
					userInput // response mode routing (report vs conversational)
			);
			List<String> toolsUsed = AgentConsole.endToolTracking();
			log.info("[NOVA] Tools used this turn: {} | JanusGraph topology RCA invoked: {}",
					toolsUsed, AgentConsole.isJanusGraphEvidenceCalled());
			return response;
		}
		catch (Exception e) {
			List<String> toolsUsed = AgentConsole.endToolTracking();
			log.info("[NOVA] Tools used this turn before failure: {} | JanusGraph topology RCA invoked: {}",
					toolsUsed, AgentConsole.isJanusGraphEvidenceCalled());
			return "[Error during deterministic PM/KPI orchestration: " + e.getMessage() + "]";
		}
	}


	private static String extractFirstIpv4(String input) {
		if (input == null || input.isBlank()) return null;
		Matcher ipMatcher = Pattern.compile("\\b\\d{1,3}(?:\\.\\d{1,3}){3}\\b").matcher(input);
		return ipMatcher.find() ? ipMatcher.group().trim() : null;
	}

	private static boolean isPmKpiIntent(String input) {
		if (input == null || input.isBlank()) return false;
		String lower = input.toLowerCase(Locale.ROOT);
		return lower.contains("availability")
				|| lower.contains("kpi")
				|| lower.contains("performance")
				|| lower.contains("throughput")
				|| lower.contains("utilization")
				|| lower.contains("latency")
				|| lower.contains("packet loss")
				|| lower.contains("erab")
				|| lower.contains("drop rate")
				|| lower.contains("pm ");
	}

	private static String buildAlarmFetchSql() {
		String sql = DEFAULT_ALARM_FILTER_SQL;
		log.info("[NOVA] Deterministic alarm SQL built: {}", sql.replace('\n', ' '));
		return sql;
	}

	private static String buildAlarmHierarchySeedSql() {
		String sql = DEFAULT_ALARM_HIERARCHY_SEED_SQL;
		log.info("[NOVA] Alarm hierarchy seed SQL built: {}", sql.replace('\n', ' '));
		return sql;
	}

	private static String shrinkAlarmDataForLlm(String raw, int maxChars) {
		if (raw == null) return "";
		if (raw.length() <= maxChars) return raw;

		// Keep markdown table header + first rows until char budget.
		String[] lines = raw.split("\\r?\\n");
		StringBuilder sb = new StringBuilder(Math.min(maxChars + 256, raw.length()));
		int tableLineCount = 0;
		for (String line : lines) {
			if (sb.length() + line.length() + 1 > maxChars) break;
			sb.append(line).append('\n');
			if (line.trim().startsWith("|")) tableLineCount++;
		}
		sb.append("\n[TRUNCATED_FOR_LLM: original chars=")
				.append(raw.length())
				.append(", sent chars=")
				.append(sb.length())
				.append(", tableLinesKept=")
				.append(tableLineCount)
				.append("]\n");
		return sb.toString();
	}

}
