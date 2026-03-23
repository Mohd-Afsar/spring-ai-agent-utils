package org.springaicommunity.nova;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import java.util.Optional;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.io.Resource;
import org.springframework.util.StringUtils;
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

	private static final Pattern RETRY_AFTER_TRY_AGAIN = Pattern.compile(
			"(?i)try\\s+again\\s+in\\s+([0-9]+(?:\\.[0-9]+)?)\\s*s(?:ec(?:onds)?)?");

	private static final Pattern RETRY_AFTER_JSON = Pattern.compile("(?i)\"retry_after\"\\s*:\\s*([0-9]+)");

	public static void main(String[] args) {
		SpringApplication.run(NovaApplication.class, args);
	}

	@Bean
	CommandLineRunner commandLineRunner(ChatClient.Builder chatClientBuilder,
			@Value("classpath:/prompt/NOVA_SYSTEM_PROMPT.md") Resource systemPrompt,
			@Value("${agent.model:openai/gpt-oss-120b}") String agentModel,
			@Value("${agent.model.knowledge.cutoff:Unknown}") String agentModelKnowledgeCutoff,
			@Value("${pm.api.base-url:http://localhost:8080}") String pmApiBaseUrl,
			@Value("${nova.topology.max-alarming-nodes:0}") int novaTopologyMaxAlarmingNodes,
			@Value("${nova.topology.max-topology-chars-for-network-intelligence:100000}") int novaTopologyMaxCharsForNi,
			@Value("${nova.topology.max-children-per-node:0}") int novaTopologyMaxChildrenPerNode,
			@Value("${nova.topology.max-link-neighbors-per-node:0}") int novaTopologyMaxLinkNeighbors,
			@Value("${nova.topology.roll-up-interface-alarms-to-parent-equipment:true}") boolean novaTopologyRollUpToEquipment,
			@Value("${nova.db.query.max-rows:0}") int novaDbQueryMaxRows,
			@Value("${nova.llm.rate-limit.max-retries:6}") int novaLlmRateLimitMaxRetries,
			@Value("${nova.llm.rate-limit.initial-wait-seconds:15}") int novaLlmRateLimitInitialWaitSeconds,
			@Value("${nova.llm.rate-limit.max-wait-seconds:120}") int novaLlmRateLimitMaxWaitSeconds,
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
		int dbMaxRows = novaDbQueryMaxRows <= 0 ? 0 : novaDbQueryMaxRows;
		DatabaseTools databaseTools = DatabaseTools.builder(dataSource).maxRows(dbMaxRows).build();
		AlarmAnalystTool alarmAnalystTool = AlarmAnalystTool.builder(subAgentBuilder.clone())
				.graphTopologyService(graphServiceOrNull)
				.build();
		KPIAnalystTool kpiAnalystTool = KPIAnalystTool.builder(subAgentBuilder.clone()).build();
		NetworkIntelligenceTool networkIntelligenceTool = NetworkIntelligenceTool.builder(subAgentBuilder.clone()).build();
		PmDataFetchTool pmDataFetchTool = PmDataFetchTool.builder()
				.pmApiBaseUrl(pmApiBaseUrl)
				.analyticsEngine(analyticsEngine)
				.chatClientBuilder(subAgentBuilder.clone())
				.build();
		ReportFormatterTool reportFormatterTool = ReportFormatterTool.builder(subAgentBuilder.clone())
				.graphTopologyService(graphServiceOrNull)
				.build();
		NetworkTopologyRcaTool networkTopologyRcaTool = (graphServiceOrNull != null)
				? NetworkTopologyRcaTool.of(graphServiceOrNull, subAgentBuilder.clone(), novaTopologyMaxAlarmingNodes,
						novaTopologyMaxChildrenPerNode, novaTopologyMaxLinkNeighbors, novaTopologyMaxCharsForNi, dataSource,
						novaTopologyRollUpToEquipment)
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

				// Tool loop must keep assistant + tool messages between iterations.
				// conversationHistoryEnabled(false) only sends system + last message and breaks
				// multi-step alarm workflows (model often ends with empty .content()).
				.defaultAdvisors(
					ToolCallAdvisor.builder().build(),
					MessageChatMemoryAdvisor.builder(MessageWindowChatMemory.builder().maxMessages(300).build())
						.order(Ordered.HIGHEST_PRECEDENCE + 1000)
						.build())

				.build();

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
					String response = callWithRetry(nova, input, novaLlmRateLimitMaxRetries,
							novaLlmRateLimitInitialWaitSeconds, novaLlmRateLimitMaxWaitSeconds);
					log.info("------------FINAL RESPONSE-------------");
					log.info("RESPONSE: {}", response);
					log.info("------------FINAL RESPONSE END-------------");
				}
			}
		};
	}

	/**
	 * {@link ChatClient.CallResponseSpec#content()} uses only {@link ChatResponse#getResult()};
	 * some providers return multiple generations or empty primary text — scan newest first.
	 */
	private static String lastNonBlankAssistantText(ChatResponse response) {
		if (response == null || response.getResults() == null || response.getResults().isEmpty()) {
			return null;
		}
		List<Generation> results = response.getResults();
		for (int i = results.size() - 1; i >= 0; i--) {
			Generation g = results.get(i);
			if (g.getOutput() != null && StringUtils.hasText(g.getOutput().getText())) {
				return g.getOutput().getText();
			}
		}
		return null;
	}

	/**
	 * Best-effort seconds hint from provider error text (e.g. Groq/OpenAI bodies).
	 */
	private static int parseRetryAfterSeconds(Throwable e) {
		int best = -1;
		for (Throwable t = e; t != null; t = t.getCause()) {
			String m = t.getMessage();
			if (m == null || m.isEmpty()) {
				continue;
			}
			Matcher j = RETRY_AFTER_JSON.matcher(m);
			if (j.find()) {
				best = Math.max(best, Integer.parseInt(j.group(1)));
			}
			Matcher a = RETRY_AFTER_TRY_AGAIN.matcher(m);
			if (a.find()) {
				best = Math.max(best, (int) Math.ceil(Double.parseDouble(a.group(1))));
			}
		}
		return best;
	}

	/**
	 * Runs NOVA with a progress ticker; on 429 / over-capacity uses exponential backoff.
	 * One user turn can be many LLM requests (tool loop + specialists), so waits must be long enough
	 * for Groq TPM/RPM windows to recover.
	 */
	private static String callWithRetry(ChatClient nova, String input, int maxRetries, int initialWaitSec, int maxWaitSec)
			throws InterruptedException {

		int safeMaxRetries = Math.max(1, maxRetries);
		int safeInitial = Math.max(1, initialWaitSec);
		int safeMaxWait = Math.max(safeInitial, maxWaitSec);
		int backoffSec = safeInitial;

		for (int attempt = 1; attempt <= safeMaxRetries; attempt++) {
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

				var callResponse = nova.prompt(input).call();
				String result = callResponse.content();
				if (!StringUtils.hasText(result)) {
					result = lastNonBlankAssistantText(callResponse.chatResponse());
				}
				if (!StringUtils.hasText(result)) {
					log.warn("[NOVA] Empty assistant text after tool loop — check model response / tool chain");
					result = "[NOVA produced no text after running tools. Try again, or narrow the request (e.g. region or severity).]";
				}
				
				done.set(true);
				progress.join();
				List<String> toolsUsed = AgentConsole.endToolTracking();
				boolean janusGraphUsed = toolsUsed.stream().anyMatch("NetworkTopologyRca"::equals)
						|| AgentConsole.isJanusGraphEvidenceCalled();
				log.info("[NOVA] Tools used this turn: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);
				return result;
			}
			catch (Exception e) {
				done.set(true);
				progress.join();
				List<String> toolsUsed = AgentConsole.endToolTracking();
				boolean janusGraphUsed = toolsUsed.stream().anyMatch("NetworkTopologyRca"::equals)
						|| AgentConsole.isJanusGraphEvidenceCalled();
				log.info("[NOVA] Tools used this turn before failure: {} | JanusGraph topology RCA invoked: {}", toolsUsed, janusGraphUsed);

				String msg = e.getMessage() != null ? e.getMessage() : "";
				boolean isRateLimit = msg.contains("429") || msg.contains("rate_limit_exceeded")
						|| msg.contains("Rate limit") || msg.contains("Too Many Requests");
				// 503 = model over capacity — treat as retriable, but with longer wait
				boolean isOverCapacity = msg.contains("503") || msg.contains("over capacity")
						|| msg.contains("TransientAiException");

				if ((isRateLimit || isOverCapacity) && attempt < safeMaxRetries) {
					int hint = parseRetryAfterSeconds(e);
					int wait = isOverCapacity ? Math.max(backoffSec, 30) : backoffSec;
					if (hint > 0) {
						wait = Math.max(wait, hint);
					}
					wait = Math.min(wait, safeMaxWait);
					String reason = isOverCapacity ? "Model over capacity" : "Rate limited";
					System.out.printf("[%s — waiting %ds before retry %d/%d]%n",
							reason, wait, attempt, safeMaxRetries);
					Thread.sleep(wait * 1000L);
					backoffSec = Math.min(backoffSec * 2, safeMaxWait);
				}
				else if (isRateLimit || isOverCapacity) {
					return "[" + (isOverCapacity ? "Model over capacity" : "Rate limit")
							+ " not resolved after " + safeMaxRetries + " attempts. "
							+ "Wait a few minutes, reduce scope (e.g. LIMIT in SQL), or raise nova.llm.rate-limit.* in application.properties.]";
				}
				else if (msg.contains("context_length_exceeded")
						|| msg.contains("reduce the length of the messages")
						|| msg.contains("context length")) {
					return "[Context limit exceeded — the next request to the model was too large. Common causes: "
							+ "(1) a wide DbQuery with many rows/columns — use smaller LIMIT, fewer columns, COUNT/GROUP BY first; "
							+ "(2) NetworkTopologyRca returning huge inventory + JSON (100k–300k+ chars) — fewer alarming NE seeds, "
							+ "nova.topology.topology-output-style=minimal, or caps like nova.topology.max-children-per-node / max-alarming-nodes in application.properties.]";
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

	private static boolean isJanusGraphInventoryRequest(String input) {
		if (input == null) return false;
		String lower = input.toLowerCase();
		return (lower.contains("janusgraph") || lower.contains("graph"))
				&& (lower.contains("how many") || lower.contains("count"))
				&& (lower.contains("node") || lower.contains("interface"));
	}

}
