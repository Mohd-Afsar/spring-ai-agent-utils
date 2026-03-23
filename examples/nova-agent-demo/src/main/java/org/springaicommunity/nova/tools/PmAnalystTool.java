package org.springaicommunity.nova.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springaicommunity.nova.AgentConsole;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.util.Assert;
import org.springframework.web.client.HttpClientErrorException;

/**
 * Specialist sub-agent for Performance Management (PM) data analysis.
 *
 * <p>NOVA passes enriched PM JSON (from {@code GET /pm/data/enriched} or
 * {@code POST /pm/data/query/enriched}) to this tool. The agent uses the
 * KPI time-series, formula metadata, static baselines, and location context
 * to produce dashboard-quality analysis covering anomaly detection, breach
 * classification, resource ranking, performance scoring, and regional health.
 *
 * @author Spring AI Community
 */
public class PmAnalystTool {

	private static final String SYSTEM_PROMPT = """
			You are a senior Telecom NOC analyst.

			You receive a pre-computed PM analytics summary (PmNodeSummary) produced by a Java \
			analytics engine from Cassandra time-series data. The summary already contains:
			- Node identity and location
			- Health status and performance score (0-100)
			- Dimension scores (availability, throughput, reliability, resource efficiency)
			- Detected anomalies with KPI name, type (SPIKE/SUSTAINED_HIGH/GRADUAL_INCREASE/DIP), \
			  severity (CRITICAL/HIGH/MEDIUM/LOW), deviation%, trend, and timestamp
			- Top busiest time periods
			- Pre-computed findings

			Your job is to turn this into a clean, professional NOC performance report.
			Write for a NOC manager who needs to act fast — be direct, precise, and jargon-appropriate.

			Rules:
			- Use only the values already in the summary — never invent numbers
			- Lead with the most critical issues
			- Explain what each anomaly means operationally (not just what the numbers say)
			- End with 3-5 concrete, prioritised actions with clear owners and timelines
			- Keep the report concise — a NOC manager should read it in under 2 minutes
			""";

	private static final Logger log = LoggerFactory.getLogger(PmAnalystTool.class);

	private static final int MAX_RETRIES = 4;

	private final ChatClient chatClient;

	private PmAnalystTool(ChatClient.Builder builder) {
		this.chatClient = builder.defaultSystem(SYSTEM_PROMPT).build();
	}

	private String callWithRetry(String prompt) {
		int waitSeconds = 2;
		System.out.println();
		System.out.println("┌─ PmAnalyst INPUT " + "─".repeat(50));
		System.out.println(prompt.length() > 3000 ? prompt.substring(0, 3000) + "\n... [truncated]" : prompt);
		System.out.println("└" + "─".repeat(68));
		for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
			try {
				String result = this.chatClient.prompt(prompt).call().content();
				System.out.println();
				System.out.println("┌─ PmAnalyst OUTPUT " + "─".repeat(49));
				System.out.println(result);
				System.out.println("└" + "─".repeat(68));
				return result;
			}
			catch (Exception e) {
				String msg = e.getMessage() != null ? e.getMessage() : "";
				boolean isRateLimit = (e instanceof HttpClientErrorException httpErr
						&& httpErr.getStatusCode().value() == 429)
						|| msg.contains("429") || msg.contains("rate_limit_exceeded");
				boolean isOverCapacity = msg.contains("503") || msg.contains("over capacity")
						|| msg.contains("TransientAiException");
				if ((!isRateLimit && !isOverCapacity) || attempt == MAX_RETRIES) {
					throw e;
				}
				int wait = isOverCapacity ? Math.max(waitSeconds * 2, 30) : waitSeconds;
				String reason = isOverCapacity ? "Model over capacity" : "Rate limited";
				log.warn("[PmAnalyst] {} — waiting {}s before retry {}/{}", reason, wait, attempt, MAX_RETRIES);
				try { Thread.sleep(wait * 1000L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
				waitSeconds = Math.min(waitSeconds * 2, 60);
			}
		}
		throw new IllegalStateException("Rate limit not resolved after " + MAX_RETRIES + " retries");
	}

	// @formatter:off
	@Tool(name = "PmAnalyst", description = """
			Generates a professional NOC performance report from pre-computed PM analytics.
			THIS IS THE FINAL STEP for all PM performance requests. After this tool returns,
			present its output directly to the user. DO NOT call ReportFormatter afterward.

			ALWAYS call PmDataFetch first. Pass the DATA_REF:nnn key it returns as enrichedPmJson.

			The analytics engine has already computed: anomalies (with severity and deviation%),
			performance score, health status, dimension scores, busiest periods, and findings.
			This tool turns those into a clear, actionable NOC report.

			Do NOT call without a DATA_REF key from PmDataFetch.
			Do NOT try to generate PM analysis yourself — only this tool can do it.
			Do NOT pass this tool's output to ReportFormatter — it is already a complete report.
			""")
	public String analyzePerformanceData( // @formatter:on
			@ToolParam(description = "The DATA_REF:nnn reference key returned by PmDataFetch, or enriched PM JSON if passed directly by the user") String enrichedPmJson,
			@ToolParam(description = "Investigation context: what is being investigated, which node/region, what time window, what triggered this analysis. Include any alarm or topology context here if available.") String investigationContext) {

		// Resolve DATA_REF:nnn cache key produced by PmDataFetchTool.
		// Falls back to the raw value if NOVA passed JSON directly (e.g. user paste).
		String resolvedJson = AgentConsole.resolveData(enrichedPmJson);
		if (resolvedJson == null) {
			return "[PmAnalyst error: data reference '" + enrichedPmJson + "' not found in cache. "
					+ "Please call PmDataFetch again to refresh the data.]";
		}

		String prompt = String.format("""
				Investigation context: %s

				Pre-computed PM analytics summary (JSON):
				%s

				Generate the NOC performance report.
				""", investigationContext, resolvedJson);

		AgentConsole.toolStarted("PmAnalyst");
		try {
			return callWithRetry(prompt);
		}
		finally {
			AgentConsole.toolFinished();
		}
	}

	public static Builder builder(ChatClient.Builder chatClientBuilder) {
		return new Builder(chatClientBuilder);
	}

	public static class Builder {

		private final ChatClient.Builder chatClientBuilder;

		private Builder(ChatClient.Builder chatClientBuilder) {
			Assert.notNull(chatClientBuilder, "chatClientBuilder must not be null");
			this.chatClientBuilder = chatClientBuilder;
		}

		public PmAnalystTool build() {
			return new PmAnalystTool(this.chatClientBuilder.clone());
		}

	}

}
