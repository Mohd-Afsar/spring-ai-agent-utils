package org.springaicommunity.nova.tools;

import org.springaicommunity.nova.AgentConsole;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/**
 * Specialist sub-agent for alarm analysis and correlation.
 *
 * <p>NOVA calls this tool after fetching raw alarm data from the database.
 * The agent applies telecom-domain expertise to identify alarm patterns,
 * severity distribution, vendor-specific fault signatures, and storm risk.
 *
 * @author Spring AI Community
 */
public class AlarmAnalystTool {

	private static final Logger log = LoggerFactory.getLogger(AlarmAnalystTool.class);

	private static final String NOC_SYSTEM_PROMPT = loadNocSystemPromptFromClasspath();

	private static String loadNocSystemPromptFromClasspath() {
		try (InputStream in = AlarmAnalystTool.class.getResourceAsStream("/prompt/ALARM_ANALYST_NOC_SYSTEM_PROMPT.md")) {
			if (in == null) {
				return fallbackNocSystemPrompt();
			}
			return new String(in.readAllBytes(), StandardCharsets.UTF_8);
		}
		catch (IOException e) {
			LoggerFactory.getLogger(AlarmAnalystTool.class).warn("[AlarmAnalyst] Could not load NOC system prompt from classpath: {}", e.getMessage());
			return fallbackNocSystemPrompt();
		}
	}

	private static String fallbackNocSystemPrompt() {
		return """
				You are AlarmAnalyst, a senior Telecom NOC Manager. Analyze alarms top-down using any topology evidence provided.
				Prefer transport/power over service alarms; treat parent/ancestor faults as root cause over child symptoms.
				Output: rootCause, impactAnalysis, alarmCorrelation, faultClassification, recommendedActions, confidence, then a brief summary.
				""";
	}

	private final ChatClient chatClient;

	private static final int LLM_MAX_RETRIES = 3;

	private static final int LLM_MAX_WAIT_SECONDS = 30;

	private AlarmAnalystTool(ChatClient.Builder builder) {
		this.chatClient = builder.defaultSystem(NOC_SYSTEM_PROMPT).build();
	}

	// @formatter:off
	@Tool(name = "AlarmAnalyst", description = """
			Senior NOC-style alarm correlation using filtered active alarm data.
			Call AFTER DbQuery/DbSample on ALARM.
			Output: rootCause, impactAnalysis, alarmCorrelation, faultClassification, recommendedActions, confidence, then summary.
			""")
	public String analyzeAlarms( // @formatter:on
			@ToolParam(description = "Raw alarm data as fetched from the database (markdown table format)") String rawAlarmData,
			@ToolParam(description = "Investigation context: scope, time window, region, user goal") String investigationContext,
			@ToolParam(description = "Optional topology context, currently unused.", required = false) String topologyHierarchyContext) {
		log.info("[AlarmAnalyst] analyzeAlarms called. rawAlarmDataChars={} topologyContextChars={}",
				rawAlarmData == null ? 0 : rawAlarmData.length(),
				topologyHierarchyContext == null ? 0 : topologyHierarchyContext.length());

		String prompt = String.format("""
				### investigationContext
				%s

				### topologyLinkEvidence
				%s

				### rawAlarmData
				%s

				Follow the system instructions. First emit the **STRICT OUTPUT FORMAT** block (rootCause through confidence), then a short **Human-readable summary** for handoff.
				Do not paste the full raw table again.
				""",
				investigationContext != null ? investigationContext : "",
				topologyHierarchyContext != null && !topologyHierarchyContext.isBlank() ? topologyHierarchyContext : "_(no topology evidence supplied)_",
				rawAlarmData != null ? rawAlarmData : "");

		AgentConsole.toolStarted("AlarmAnalyst");
		try {
			return callWithRateLimitRetries(() -> this.chatClient.prompt(prompt).call().content());
		}
		finally {
			AgentConsole.toolFinished();
		}
	}

	private static String callWithRateLimitRetries(Supplier<String> call) {
		int waitSec = 2;
		Exception last = null;
		for (int attempt = 1; attempt <= LLM_MAX_RETRIES; attempt++) {
			try {
				return call.get();
			}
			catch (Exception e) {
				last = e;
				String msg = e.getMessage() != null ? e.getMessage() : "";
				boolean rate = msg.contains("429") || msg.contains("rate_limit_exceeded") || msg.contains("Rate limit");
				boolean cap = msg.contains("503") || msg.contains("over capacity") || msg.contains("TransientAiException");
				if ((rate || cap) && attempt < LLM_MAX_RETRIES) {
					int wait = cap ? Math.max(waitSec * 2, 30) : Math.min(waitSec, LLM_MAX_WAIT_SECONDS);
					log.warn("[AlarmAnalyst] {} — sleeping {}s then retry {}/{}",
							cap ? "Model over capacity" : "Rate limited", wait, attempt, LLM_MAX_RETRIES);
					try {
						Thread.sleep(wait * 1000L);
					}
					catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						return "[AlarmAnalyst interrupted during rate-limit backoff]";
					}
					waitSec = Math.min(waitSec * 2, LLM_MAX_WAIT_SECONDS);
				}
				else {
					break;
				}
			}
		}
		String m = last != null && last.getMessage() != null ? last.getMessage() : "unknown error";
		return "[AlarmAnalyst error after retries: " + m + "]";
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

		public AlarmAnalystTool build() {
			return new AlarmAnalystTool(this.chatClientBuilder.clone());
		}

	}

}
