package org.springaicommunity.nova;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.Locale;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import java.util.List;

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
import java.util.Optional;
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
	/** Cap topology markdown inside AlarmAnalyst user message; full topology still appended after analysis. */
	private static final int MAX_TOPOLOGY_CHARS_INSIDE_ALARM_ANALYST_PROMPT = 14000;

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
		System.out.println("graphTopologyService: " + graphTopologyService);
		System.out.println("--------------------------------\n\n\n");

		// Clone the builder BEFORE adding any NOVA-specific tools/system prompt.
		// Sub-agents must get a clean, tool-free builder so their LLM cannot call
		// DbQuery, ReportFormatter, or any other NOVA tool — it must only generate text.
		ChatClient.Builder subAgentBuilder = chatClientBuilder.clone();
		GraphTopologyService graphServiceOrNull = graphTopologyService.orElse(null);
		DatabaseTools databaseTools = DatabaseTools.builder(dataSource).maxRows(500).build();
		AlarmAnalystTool alarmAnalystTool = AlarmAnalystTool.builder(subAgentBuilder.clone())
				.graphTopologyService(graphServiceOrNull)
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
				.graphTopologyService(graphServiceOrNull)
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
			// Note: ReportFormatterTool also performs optional JanusGraph evidence
			// enrichment as a fallback when AlarmAnalyst wasn't called.
			// It requires GraphTopologyService injection.

		// Layer 3: JanusGraph topology RCA — only registered when janusgraph.enabled=true
		graphTopologyService.ifPresent(service -> {
			novaBuilder.defaultTools(networkTopologyRcaTool);
			System.out.println("║  Graph RCA:  NetworkTopologyRca (JanusGraph)         ║");
		});

		ChatClient nova = novaBuilder

				.defaultAdvisors(
					ToolCallAdvisor.builder()
						.conversationHistoryEnabled(false)
						.build(),
					MessageChatMemoryAdvisor.builder(MessageWindowChatMemory.builder().maxMessages(300).build())
						.order(Ordered.HIGHEST_PRECEDENCE + 1000)
						.build())

				.build();
			log.info("[NOVA] GraphTopologyService available: {}", graphServiceOrNull != null);
			log.info("[NOVA] Registered topology tool: {}", graphServiceOrNull != null ? "NetworkTopologyRca" : "none");

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
					if (isJanusGraphInventoryRequest(input)) {
						String response = graphServiceOrNull != null
								? graphServiceOrNull.inventorySummary()
								: "JanusGraph is disabled or not configured (graphTopologyService is unavailable).";
						System.out.println("\nNOVA: " + response);
						continue;
					}
					if (isAlarmAnalysisRequest(input)) {
						log.info("[NOVA] Running deterministic alarm orchestration for input='{}'", input);
						String response = runDeterministicAlarmFlow(input, dataSource, databaseTools, alarmAnalystTool, networkTopologyRcaTool);
						System.out.println("\nNOVA: " + response);
						continue;
					}
					if (isPmKpiIntent(input)) {
						log.info("[NOVA][Routing] Running deterministic PM/KPI orchestration for input='{}'", input);
						String response = runDeterministicPmFlow(input, pmDataFetchTool);
						System.out.println("\nNOVA: " + response);
						continue;
					}
					if (isTopologyConnectivityIntent(input)) {
						log.info("[NOVA][Routing] Running deterministic topology-connectivity orchestration for input='{}'", input);
						String response = runDeterministicConnectivityFlow(input, networkTopologyRcaTool);
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
		boolean topologyIntent = isTopologyConnectivityIntent(input);
		if (topologyIntent) {
			log.info("[NOVA][Routing] Topology-connectivity intent detected for input='{}'", input);
		}

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
				boolean janusGraphUsed = toolsUsed.stream().anyMatch("NetworkTopologyRca"::equals)
						|| AgentConsole.isJanusGraphEvidenceCalled();
				log.info("[NOVA] Tools used this turn: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);
				if (topologyIntent && !janusGraphUsed) {
					log.warn("[NOVA][Routing] Topology intent detected but JanusGraph tool not invoked. input='{}' toolsUsed={} -> likely model selected DB-only path or intent ambiguity. Consider explicit wording like 'use JanusGraph topology' to force tool routing.",
							input, toolsUsed);
				}
				return result;
			}
			catch (Exception e) {
				done.set(true);
				progress.join();
				List<String> toolsUsed = AgentConsole.endToolTracking();
				boolean janusGraphUsed = toolsUsed.stream().anyMatch("NetworkTopologyRca"::equals)
						|| AgentConsole.isJanusGraphEvidenceCalled();
				log.info("[NOVA] Tools used this turn before failure: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);
				if (topologyIntent && !janusGraphUsed) {
					log.warn("[NOVA][Routing] Topology intent detected before failure, but JanusGraph tool was still not invoked. input='{}' toolsUsed={}",
							input, toolsUsed);
				}

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

	private static boolean isJanusGraphInventoryRequest(String input) {
		if (input == null) return false;
		String lower = input.toLowerCase();
		return (lower.contains("janusgraph") || lower.contains("graph"))
				&& (lower.contains("how many") || lower.contains("count"))
				&& (lower.contains("node") || lower.contains("interface"));
	}

	private static boolean isTopologyConnectivityIntent(String input) {
		if (input == null || input.isBlank()) return false;
		String lower = input.toLowerCase(Locale.ROOT);
		boolean hasConnectivityVerb = lower.contains("connected")
				|| lower.contains("connectivity")
				|| lower.contains("neighbour")
				|| lower.contains("neighbor")
				|| lower.contains("adjacent")
				|| lower.contains("linked")
				|| lower.contains("topology");
		boolean hasEntityHint = lower.contains("node")
				|| lower.contains("ip")
				|| lower.matches(".*\\b\\d{1,3}(\\.\\d{1,3}){3}\\b.*");
		return hasConnectivityVerb && hasEntityHint;
	}

	private static String runDeterministicAlarmFlow(String userInput,
			DataSource dataSource,
			DatabaseTools databaseTools,
			AlarmAnalystTool alarmAnalystTool,
			NetworkTopologyRcaTool networkTopologyRcaTool) {
		AgentConsole.beginToolTracking();
		try {
			String sql = buildAlarmFetchSql(dataSource);
			String rawAlarmData = databaseTools.executeQuery(sql);

			List<String> interfaces = extractDistinctFromMarkdownTable(rawAlarmData,
					List.of("interface", "ifname", "if_name", "port", "port_name", "source_interface"));

			String scope = "overall active alarms";
			String investigationContext = scope + " | userRequest='" + userInput + "'";
			if (!interfaces.isEmpty()) {
				investigationContext += " | interfaceSample=" + String.join(", ", interfaces.stream().toList());
			}

			String topologyEvidence;
			if (networkTopologyRcaTool == null) {
				topologyEvidence = "_JanusGraph topology RCA is disabled (janusgraph.enabled=false)._";
			}
			else {
				// Full alarm markdown → parse topology-related columns → MySQL NE_NAME → JanusGraph structure for LLM RCA
				topologyEvidence = networkTopologyRcaTool.analyzeTopologyForAlarmMarkdown(rawAlarmData, investigationContext);
			}
			// Never log full topology at INFO — it floods the console and duplicates the LLM payload.
			log.info("[NOVA] Topology evidence built: chars={}", topologyEvidence != null ? topologyEvidence.length() : 0);
			if (log.isDebugEnabled() && topologyEvidence != null) {
				int n = Math.min(500, topologyEvidence.length());
				log.debug("[NOVA] Topology evidence preview (first {} chars): {}", n,
						topologyEvidence.substring(0, n).replace("\n", "\\n"));
			}

			String alarmDataForLlm = shrinkAlarmDataForLlm(rawAlarmData, MAX_ALARM_DATA_CHARS_FOR_LLM);
			if (alarmDataForLlm.length() < rawAlarmData.length()) {
				log.warn("[NOVA] Truncated alarm data for AlarmAnalyst. originalChars={} sentChars={}",
						rawAlarmData.length(), alarmDataForLlm.length());
			}
			String topologyForAnalystPrompt = topologyEvidence;
			if (topologyForAnalystPrompt != null && topologyForAnalystPrompt.length() > MAX_TOPOLOGY_CHARS_INSIDE_ALARM_ANALYST_PROMPT) {
				topologyForAnalystPrompt = topologyForAnalystPrompt.substring(0, MAX_TOPOLOGY_CHARS_INSIDE_ALARM_ANALYST_PROMPT)
						+ "\n\n[TOPOLOGY_TRUNCATED_FOR_LLM: full copy below under 'Topology Hierarchy Evidence']\n";
				log.warn("[NOVA] Truncated topology inside AlarmAnalyst prompt. sentChars={} fullChars={}",
						MAX_TOPOLOGY_CHARS_INSIDE_ALARM_ANALYST_PROMPT, topologyEvidence.length());
			}
			String alarmAnalysis = alarmAnalystTool.analyzeAlarms(alarmDataForLlm, investigationContext,
					topologyForAnalystPrompt != null ? topologyForAnalystPrompt : "");
			String merged = alarmAnalysis + "\n\n---\n\n### Topology Hierarchy Evidence\n\n" + topologyEvidence;

			List<String> toolsUsed = AgentConsole.endToolTracking();
			boolean janusGraphUsed = toolsUsed.stream().anyMatch("NetworkTopologyRca"::equals)
					|| AgentConsole.isJanusGraphEvidenceCalled();
			log.info("[NOVA] Tools used this turn: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);
			return merged;
		}
		catch (Exception e) {
			List<String> toolsUsed = AgentConsole.endToolTracking();
			boolean janusGraphUsed = toolsUsed.stream().anyMatch("NetworkTopologyRca"::equals)
					|| AgentConsole.isJanusGraphEvidenceCalled();
			log.info("[NOVA] Tools used this turn before failure: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);
			return "[Error during deterministic alarm orchestration: " + e.getMessage() + "]";
		}
	}

	private static String runDeterministicConnectivityFlow(String userInput,
			NetworkTopologyRcaTool networkTopologyRcaTool) {
		AgentConsole.beginToolTracking();
		try {
			if (networkTopologyRcaTool == null) {
				List<String> toolsUsed = AgentConsole.endToolTracking();
				log.info("[NOVA] Tools used this turn: {} | JanusGraph topology RCA invoked: false", toolsUsed);
				return "JanusGraph topology RCA is disabled or unavailable. Enable `janusgraph.enabled=true` and verify Gremlin connectivity.";
			}

			List<String> seeds = extractConnectivitySeeds(userInput);
			if (seeds.isEmpty()) {
				List<String> toolsUsed = AgentConsole.endToolTracking();
				log.info("[NOVA] Tools used this turn: {} | JanusGraph topology RCA invoked: false", toolsUsed);
				return "I could not extract a node/IP from your query. Please provide an NE_NAME, nodeId, or IPv4 (e.g., `172.31.31.131`).";
			}

			String alarmingNodeIds = String.join(", ", seeds);
			String context = "deterministic-connectivity-route | userRequest='" + userInput + "'";
			log.info("[NOVA][Routing] JanusGraph deterministic request -> tool=NetworkTopologyRca alarmingNodeIds='{}' context='{}'",
					alarmingNodeIds, context);
			String topology = networkTopologyRcaTool.analyzeTopologyRootCause(alarmingNodeIds, context);

			List<String> toolsUsed = AgentConsole.endToolTracking();
			boolean janusGraphUsed = toolsUsed.stream().anyMatch("NetworkTopologyRca"::equals)
					|| AgentConsole.isJanusGraphEvidenceCalled();
			log.info("[NOVA] Tools used this turn: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);
			return topology;
		}
		catch (Exception e) {
			List<String> toolsUsed = AgentConsole.endToolTracking();
			boolean janusGraphUsed = toolsUsed.stream().anyMatch("NetworkTopologyRca"::equals)
					|| AgentConsole.isJanusGraphEvidenceCalled();
			log.info("[NOVA] Tools used this turn before failure: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);
			return "[Error during deterministic topology connectivity orchestration: " + e.getMessage() + "]";
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

	private static List<String> extractConnectivitySeeds(String input) {
		if (input == null || input.isBlank()) return List.of();

		LinkedHashSet<String> seeds = new LinkedHashSet<>();
		Matcher ipMatcher = Pattern.compile("\\b\\d{1,3}(?:\\.\\d{1,3}){3}\\b").matcher(input);
		while (ipMatcher.find()) {
			seeds.add(ipMatcher.group().trim());
		}

		Matcher quotedMatcher = Pattern.compile("[\"'`]{1}([^\"'`]{2,})[\"'`]{1}").matcher(input);
		while (quotedMatcher.find()) {
			String value = quotedMatcher.group(1).trim();
			if (!value.isBlank()) {
				seeds.add(value);
			}
		}

		return List.copyOf(seeds);
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

	private static String buildAlarmFetchSql(DataSource dataSource) {
		Set<String> columns = getTableColumns(dataSource, "ALARM");
		String statusCol = firstExisting(columns, "alarm_status", "status", "current_status");
		String tsCol = firstExisting(columns, "last_event_time", "event_time", "updated_at", "raised_time", "created_at", "timestamp");

		StringBuilder sql = new StringBuilder("SELECT * FROM ALARM");
		if (statusCol != null) {
			sql.append(" WHERE ").append(statusCol).append(" IN ('OPEN','REOPEN')");
		}
		if (tsCol != null) {
			sql.append(" ORDER BY ").append(tsCol).append(" DESC");
		}
		String finalSql = sql.toString();
		log.info("[NOVA] Deterministic alarm SQL built: {}", finalSql);
		return finalSql;
	}

	private static Set<String> getTableColumns(DataSource dataSource, String tableName) {
		Set<String> cols = new HashSet<>();
		try (Connection conn = dataSource.getConnection()) {
			DatabaseMetaData meta = conn.getMetaData();
			String schema = conn.getSchema();
			try (ResultSet rs = meta.getColumns(null, schema, tableName, "%")) {
				while (rs.next()) {
					String col = rs.getString("COLUMN_NAME");
					if (col != null) cols.add(col.toLowerCase(Locale.ROOT));
				}
			}
		}
		catch (SQLException e) {
			log.warn("[NOVA] Could not read table metadata for {}: {}", tableName, e.getMessage());
		}
		return cols;
	}

	private static String firstExisting(Set<String> columns, String... candidates) {
		for (String candidate : candidates) {
			if (columns.contains(candidate.toLowerCase(Locale.ROOT))) {
				return candidate;
			}
		}
		return null;
	}

	private static List<String> extractDistinctFromMarkdownTable(String markdownTable, List<String> headerCandidates) {
		if (markdownTable == null || markdownTable.isBlank()) return List.of();
		String[] lines = markdownTable.split("\\r?\\n");
		List<String> rows = new ArrayList<>();
		for (String line : lines) {
			String t = line.trim();
			if (t.startsWith("|") && t.endsWith("|")) {
				rows.add(t);
			}
		}
		if (rows.size() < 3) return List.of();

		List<String> headers = splitMarkdownRow(rows.get(0));
		int idx = -1;
		for (String candidate : headerCandidates) {
			String c = normalizeHeader(candidate);
			for (int i = 0; i < headers.size(); i++) {
				String h = normalizeHeader(headers.get(i));
				if (h.equals(c) || h.contains(c)) {
					idx = i;
					break;
				}
			}
			if (idx >= 0) break;
		}
		if (idx < 0) return List.of();

		Set<String> distinct = new LinkedHashSet<>();
		for (int i = 2; i < rows.size(); i++) {
			String row = rows.get(i);
			if (row.contains("---")) continue;
			List<String> cells = splitMarkdownRow(row);
			if (idx >= cells.size()) continue;
			String value = cells.get(idx).replace("**", "").trim();
			if (value.isBlank() || "null".equalsIgnoreCase(value)) continue;
			distinct.add(value);
			if (distinct.size() >= 80) break;
		}
		return List.copyOf(distinct);
	}

	private static List<String> splitMarkdownRow(String row) {
		if (row == null || row.isBlank()) return List.of();
		String t = row.trim();
		if (!t.startsWith("|") || !t.endsWith("|")) return List.of();
		String core = t.substring(1, t.length() - 1);
		String[] parts = core.split(Pattern.quote("|"), -1);
		List<String> out = new ArrayList<>(parts.length);
		for (String part : parts) out.add(part.trim());
		return out;
	}

	private static String normalizeHeader(String s) {
		if (s == null) return "";
		return s.toLowerCase().replaceAll("[^a-z0-9]", "");
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
