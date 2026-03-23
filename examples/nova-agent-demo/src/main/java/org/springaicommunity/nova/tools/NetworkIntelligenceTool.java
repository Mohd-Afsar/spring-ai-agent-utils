package org.springaicommunity.nova.tools;

import org.springaicommunity.nova.AgentConsole;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.util.Assert;

/**
 * Specialist sub-agent for network topology analysis, root cause correlation,
 * and incident impact assessment.
 *
 * <p>NOVA calls this tool to reason about node relationships, upstream/downstream
 * dependencies, fault propagation paths, and to classify whether observed
 * events are correlated, coincidental, or cascading.
 *
 * @author Spring AI Community
 */
public class NetworkIntelligenceTool {

	private static final String SYSTEM_PROMPT = """
			You are a Telecom Network Intelligence Specialist with expertise in network topology, fault correlation, and root cause analysis (RCA) methodology.
			You receive network data (node status, incidents, topology, alarms) and produce a structured expert analysis for a NOC manager.

			Your analysis ALWAYS covers:
			1. NODE STATUS SUMMARY — up/down/degraded nodes, duration of outages
			2. FAULT CLASSIFICATION — for each observed fault group:
			   - CORRELATED: multiple symptoms from one root cause (e.g. upstream physical failure)
			   - CASCADING: one fault triggering a chain (e.g. BGP route withdrawal flooding)
			   - COINCIDENTAL: independent parallel failures (separate root causes)
			3. ROOT CAUSE HYPOTHESES — ranked by likelihood:
			   - H1 (most likely): [hypothesis] — supporting evidence
			   - H2: [hypothesis] — supporting evidence
			   - H3 if applicable
			4. UPSTREAM/DOWNSTREAM IMPACT — which nodes depend on the affected node?
			   What services are at risk if the fault propagates?
			5. FAULT PROPAGATION RISK — is this fault contained or likely to spread?
			6. GEOGRAPHY IMPACT — which regions (NORTH/SOUTH/EAST/WEST) are affected
			   or at risk? Are regional boundaries being crossed?
			7. ESCALATION TRIGGER CHECK — does any condition meet escalation criteria?
			   (node down > 2h, BGP loss on backbone, > 10 nodes down in region, etc.)
			8. RECOMMENDED NEXT STEPS — the top 3 actions for the NOC team

			Be direct. Use protocol shorthand (BGP FSM, OSPF adjacency, IS-IS LSP, BFD session, etc.).
			State confidence levels: HIGH / MEDIUM / LOW for each root cause hypothesis.
			If the data is insufficient to determine root cause, say exactly what additional
			data is needed and from which table/source.
			""";

	private final ChatClient chatClient;

	private NetworkIntelligenceTool(ChatClient.Builder builder) {
		this.chatClient = builder.defaultSystem(SYSTEM_PROMPT).build();
	}

	// @formatter:off
	@Tool(name = "NetworkIntelligence", description = """
			Specialist sub-agent for network topology analysis, root cause correlation, and incident impact assessment.

			Call this with node status, incident log, topology, or mixed network data.
			Returns structured expert analysis:
			- Node status summary (up/down/degraded with durations)
			- Fault classification (correlated vs. cascading vs. coincidental)
			- Root cause hypotheses ranked by likelihood with confidence levels
			- Upstream/downstream impact assessment
			- Fault propagation risk (contained vs. spreading)
			- Geography impact (which regions affected or at risk)
			- Escalation trigger check (automatic escalation criteria evaluation)
			- Top 3 recommended next steps for the NOC team

			Use this for: incident investigations, RCA support, node-down analysis, regional health checks, and shift handover risk assessment.
			""")
	public String analyzeNetworkIntelligence( // @formatter:on
			@ToolParam(description = "Raw network data: node status, incidents, topology, or mixed data from database queries") String rawNetworkData,
			@ToolParam(description = "Investigation scope: which nodes/region/incident, what time window, what is the suspected issue") String investigationScope,
			@ToolParam(description = "Alarm analyst findings if available (optional — helps with correlation)", required = false) String alarmAnalystFindings) {

		String alarmSection = (alarmAnalystFindings != null && !alarmAnalystFindings.isBlank())
				? "\nAlarm analyst findings:\n" + alarmAnalystFindings : "";

		String prompt = String.format("""
				Investigation scope: %s

				Raw network data from database:
				%s
				%s
				Provide your expert network intelligence analysis.
				""", investigationScope, rawNetworkData, alarmSection);

		AgentConsole.toolStarted("NetworkIntelligence");
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

		public NetworkIntelligenceTool build() {
			return new NetworkIntelligenceTool(this.chatClientBuilder.clone());
		}

	}

}
