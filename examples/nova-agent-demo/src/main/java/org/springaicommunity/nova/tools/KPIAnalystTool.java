package org.springaicommunity.nova.tools;

import org.springaicommunity.nova.AgentConsole;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.util.Assert;

/**
 * Specialist sub-agent for KPI and traffic metrics analysis.
 *
 * <p>NOVA calls this tool after fetching raw KPI data from the database.
 * The agent analyzes traffic trends, utilization spikes, DDoS signatures,
 * and capacity concerns using telecom network operations expertise.
 *
 * @author Spring AI Community
 */
public class KPIAnalystTool {

	private static final String SYSTEM_PROMPT = """
			You are a Telecom Network KPI and Traffic Analysis Specialist.
			You receive raw KPI metrics data from a telecom NOC database and produce structured expert analysis.

			Your analysis ALWAYS covers:
			1. TRAFFIC SUMMARY — inbound/outbound utilization at the affected nodes/interfaces
			2. ANOMALY DETECTION — identify deviations from expected baseline (spikes, dips, flatlines)
			3. DDOS SIGNATURE CHECK — sudden traffic spikes + utilization ceiling + peering impact
			   = DDoS indicator. Classify as: CONFIRMED / SUSPECTED / UNLIKELY
			4. UTILIZATION CONCERNS — flag any interface > 80% (warning), > 90% (critical)
			5. TREND DIRECTION — is traffic rising, falling, or stable over the observed window?
			6. CAPACITY RISK — will current trend breach capacity within the next hour/day?
			7. CORRELATION WITH ALARMS — if alarm context is provided, link KPI anomalies to alarm events
			8. RECOMMENDED ACTIONS — top 3 actions based on the KPI picture

			Use network operations shorthand freely (Mbps, Gbps, util%, Rx/Tx, BNG, CDN, peering, etc.).
			Be precise with numbers. Flag when data is insufficient to draw conclusions.
			If a DDoS is even suspected, call it out clearly — do not downplay it.
			""";

	private final ChatClient chatClient;

	private KPIAnalystTool(ChatClient.Builder builder) {
		this.chatClient = builder.defaultSystem(SYSTEM_PROMPT).build();
	}

	// @formatter:off
	@Tool(name = "KPIAnalyst", description = """
			Specialist sub-agent for KPI and traffic metrics analysis.

			Call this AFTER fetching raw KPI or metrics data from the database.
			Optionally include related alarm context to enable cross-correlation.
			Returns structured expert analysis:
			- Traffic summary (Rx/Tx utilization at node/interface level)
			- Anomaly detection (spikes, dips, flatlines vs. baseline)
			- DDoS signature assessment (CONFIRMED / SUSPECTED / UNLIKELY)
			- Utilization threshold alerts (>80% warning, >90% critical)
			- Traffic trend direction (rising / falling / stable)
			- Capacity risk projection
			- Recommended actions

			Do NOT call this without raw KPI data — always fetch with DbQuery first.
			""")
	public String analyzeKPIs( // @formatter:on
			@ToolParam(description = "Raw KPI or metrics data as fetched from the database") String rawKpiData,
			@ToolParam(description = "Context: which node/interface/region, what time window, what triggered this investigation") String investigationContext,
			@ToolParam(description = "Related alarm context if available (optional — pass empty string if not available)", required = false) String relatedAlarmContext) {

		String alarmSection = (relatedAlarmContext != null && !relatedAlarmContext.isBlank())
				? "\nRelated alarm context:\n" + relatedAlarmContext : "";

		String prompt = String.format("""
				Investigation context: %s

				Raw KPI data from database:
				%s
				%s
				Provide your expert KPI and traffic analysis.
				""", investigationContext, rawKpiData, alarmSection);

		AgentConsole.toolStarted("KPIAnalyst");
		try {
			return this.chatClient.prompt(prompt).call().content();
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

		public KPIAnalystTool build() {
			return new KPIAnalystTool(this.chatClientBuilder.clone());
		}

	}

}
