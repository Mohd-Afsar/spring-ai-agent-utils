package org.springaicommunity.nova.tools;

import org.springaicommunity.nova.AgentConsole;
import org.springaicommunity.nova.graph.GraphTopologyService;
import org.springaicommunity.nova.graph.RcaGroup;
import org.springaicommunity.nova.graph.RcaNode;
import org.springaicommunity.nova.graph.RcaResult;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Specialist sub-agent for generating structured NOC reports.
 *
 * <p>NOVA calls this tool as the final step after completing all analysis.
 * The formatter takes NOVA's findings and produces a polished, correctly
 * structured report appropriate for the target audience and report type.
 *
 * <p>The system prompt is loaded from an external skill file
 * ({@code skills/report-formatter/SKILL.md}) so report templates can be
 * updated without recompiling the application.
 *
 * @author Spring AI Community
 */
public class ReportFormatterTool {

	private static final Logger log = LoggerFactory.getLogger(ReportFormatterTool.class);

	/**
	 * Prevent "messages too large" by truncating the prompt payload.
	 * Evidence enrichment is appended *after* the LLM call.
	 */
	private static final int MAX_ANALYSIS_FINDINGS_CHARS_FOR_PROMPT = 8000;

	private final ChatClient chatClient;

	private final GraphTopologyService graphTopologyService;

	private ReportFormatterTool(ChatClient.Builder builder, String systemPrompt, GraphTopologyService graphTopologyService) {
		this.chatClient = builder.defaultSystem(systemPrompt).build();
		this.graphTopologyService = graphTopologyService;
	}

	// @formatter:off
	@Tool(name = "ReportFormatter", description = """
			Specialist sub-agent for generating polished, structured NOC reports from ALARM and INCIDENT data.

			⛔ DO NOT call this for PM performance analysis. If you have called PmDataFetch or PmAnalyst,
			their output IS the final report — present it directly to the user. Stop there.

			Only call this after gathering alarm, incident, or network intelligence findings.
			Pass your complete alarm/incident analysis findings and specify the report type.

			Report types:
			- QUICK_STATUS               : brief operational status bullets
			- SHIFT_HANDOVER             : end-of-shift handover brief for incoming team
			- MANAGEMENT_SUMMARY         : executive report with metrics and business impact
			- INCIDENT_INVESTIGATION     : structured investigation report for a specific fault
			- RCA                        : post-incident root cause analysis report
			- DDOS_ASSESSMENT            : DDoS attack assessment and mitigation report
			- REGIONAL_HEALTH            : health scorecard for a specific single region
			- REGIONAL_OVERVIEW          : executive-ready network-wide report across ALL regions
			- FREQUENT_OFFENDER_SUMMARY  : top offender node analysis report
			- NODE_DOWN_SUMMARY          : node down / device down status report
			- ALARM_SPIKE_REPORT         : alarm spike / threshold breach analysis report
			- KPI_ANOMALY_REPORT         : KPI anomaly detection results report
			- INCIDENT_CORRELATION_REPORT: incident correlation analysis report

			Returns a complete, formatted report ready for use.
			""")
	public String formatReport( // @formatter:on
			@ToolParam(description = "Complete analysis findings from your investigation (alarm analysis, KPI analysis, network intelligence findings, and your own NOC assessment)") String analysisFindings,
			@ToolParam(description = "Report type: QUICK_STATUS | SHIFT_HANDOVER | MANAGEMENT_SUMMARY | INCIDENT_INVESTIGATION | RCA | DDOS_ASSESSMENT | REGIONAL_HEALTH | REGIONAL_OVERVIEW | FREQUENT_OFFENDER_SUMMARY | NODE_DOWN_SUMMARY | ALARM_SPIKE_REPORT | KPI_ANOMALY_REPORT | INCIDENT_CORRELATION_REPORT") String reportType,
			@ToolParam(description = "Time period covered by this report (e.g. '2024-06-01 08:00 to 16:00', 'Last 24 hours', 'Week of 2024-06-03')") String period) {

		// Hard guard: if the findings look like PM analytics output, refuse and redirect.
		// This prevents NOVA from accidentally piping PmAnalyst output through here.
		if (isPmAnalyticsContent(analysisFindings)) {
			return "[ReportFormatter] This content is already a PM performance report generated " +
					"by PmAnalyst. Present it directly to the user — do not pass it here.";
		}

		System.out.println("[ReportFormatter] formatReport called. reportType=" + reportType
				+ " graphTopologyServicePresent=" + (this.graphTopologyService != null)
				+ " evidenceAlreadyCalled=" + AgentConsole.isJanusGraphEvidenceCalled());
		log.info("[ReportFormatter] formatReport called. reportType={} graphTopologyServicePresent={} evidenceAlreadyCalled={}",
				reportType, (this.graphTopologyService != null), AgentConsole.isJanusGraphEvidenceCalled());

		// Final-step hierarchy attachment:
		// If NOVA didn't call AlarmAnalyst (you may see Tools used this turn: [ReportFormatter]),
		// we still want to enrich alarm-based reports with JanusGraph hierarchy evidence.
		//
		// IMPORTANT: we do NOT append evidence into the LLM prompt, because it can push
		// the request size over model limits. We append evidence to the final output instead.
		String evidenceText = null;
		if (this.graphTopologyService != null
				&& !AgentConsole.isJanusGraphEvidenceCalled()
				&& isAlarmReportType(reportType)) {
			List<String> extractedNodeIds = extractNodeIdsFromFindings(analysisFindings, 50);
			if (!extractedNodeIds.isEmpty()) {
				AgentConsole.markJanusGraphEvidenceCalled();
				System.out.println("[ReportFormatter] calling JanusGraph evidence enrichment. nodeIdsCount=" + extractedNodeIds.size());
				log.info("[ReportFormatter] JanusGraph evidence enrichment. nodeIdsCount={} sample={}",
						extractedNodeIds.size(), evidenceSample(extractedNodeIds, 15));

				RcaResult rcaResult = this.graphTopologyService.analyzeRootCause(extractedNodeIds);
				evidenceText = formatEvidence(rcaResult);

				log.info("[ReportFormatter] JanusGraph evidence enrichment done. groups={} evidenceChars={}",
						(rcaResult.groups() == null ? 0 : rcaResult.groups().size()),
						(evidenceText == null ? 0 : evidenceText.length()));
				System.out.println("[ReportFormatter] JanusGraph evidence groups=" + (rcaResult.groups() == null ? 0 : rcaResult.groups().size()));
				log.info("[ReportFormatter] JanusGraph evidence preview: {}", evidenceSample(evidenceText, 350));
				// Save evidenceText; append to final output after the LLM call.
				System.out.println("[ReportFormatter] evidenceText computed chars=" + (evidenceText == null ? 0 : evidenceText.length()));
			} else {
				log.info("[ReportFormatter] Skipping JanusGraph enrichment: could not extract nodeIds from findings.");
			}
		}

		String analysisForPrompt = analysisFindings;
		if (analysisForPrompt != null && analysisForPrompt.length() > MAX_ANALYSIS_FINDINGS_CHARS_FOR_PROMPT) {
			analysisForPrompt = analysisForPrompt.substring(0, MAX_ANALYSIS_FINDINGS_CHARS_FOR_PROMPT)
					+ "\n\n[TRUNCATED: analysisFindings too large for prompt]";
			log.warn("[ReportFormatter] Truncated analysisFindings for prompt: originalChars={} usedChars={}",
					analysisFindings.length(), analysisForPrompt.length());
		}

		String prompt = String.format("""
				Report type requested: %s
				Period: %s

				Analysis findings from NOC Manager:
				%s

				Generate the complete formatted report.
				""", reportType, period, analysisForPrompt);

		AgentConsole.toolStarted("ReportFormatter");
		try {
			String formatted = this.chatClient.prompt(prompt).call().content();
			if (evidenceText != null && !evidenceText.isBlank()) {
				return formatted + "\n\n---\n\n" + evidenceText;
			}
			return formatted;
		}
		finally {
			AgentConsole.toolFinished();
		}
	}

	/**
	 * Detects if the supplied text is PM analytics content produced by PmAnalyst.
	 * Checks for typical field names or section headers that appear in PmNodeSummary
	 * or PmAnalyst's formatted performance report output.
	 */
	private static boolean isPmAnalyticsContent(String text) {
		if (text == null) return false;
		String lower = text.toLowerCase();
		int hits = 0;
		if (lower.contains("performancescore") || lower.contains("performance score")) hits++;
		if (lower.contains("anomalies") && lower.contains("kpicode")) hits++;
		if (lower.contains("dimensionscores") || lower.contains("dimension scores")) hits++;
		if (lower.contains("health") && lower.contains("critical") && lower.contains("score")) hits++;
		if (lower.contains("pmanalyst") || lower.contains("pmdatafetch") || lower.contains("data_ref:")) hits++;
		if (lower.contains("busiestperiods") || lower.contains("busiest period")) hits++;
		return hits >= 2;
	}

	private static boolean isAlarmReportType(String reportType) {
		if (reportType == null) return false;
		String rt = reportType.trim().toUpperCase().replace(' ', '_');
		// Be permissive: LLM sometimes passes slightly different tokens.
		return rt.contains("ALARM_SPIKE")
				|| rt.contains("FREQUENT_OFFENDER")
				|| rt.contains("REGIONAL_OVERVIEW")
				|| rt.contains("REGIONAL_HEALTH")
				|| rt.contains("NODE_DOWN")
				|| rt.contains("MANAGEMENT_SUMMARY")
				|| rt.contains("QUICK_STATUS")
				|| rt.contains("SHIFT_HANDOVER")
				|| rt.equals("RCA"); // post-incident often contains alarm-derived context
	}

	/**
	 * Extracts node IDs from a markdown report body that typically contains a table like:
	 * | Node (ENTITY_NAME) | ... |
	 * | CO_DC-... | ... |
	 */
	private static List<String> extractNodeIdsFromFindings(String analysisFindings, int limit) {
		if (analysisFindings == null || analysisFindings.isBlank()) return List.of();

		Set<String> nodeIds = new LinkedHashSet<>();
		String[] lines = analysisFindings.split("\\r?\\n");

		boolean tableHeaderSeen = false;
		for (String line : lines) {
			String t = line.trim();
			if (t.startsWith("|") && t.contains("ENTITY_NAME") && t.toLowerCase().contains("node")) {
				tableHeaderSeen = true;
				continue;
			}
			if (!tableHeaderSeen) continue;
			if (!t.startsWith("|")) continue;
			if (t.contains("---")) continue;

			// take first cell after first pipe
			String[] parts = t.substring(1, Math.max(0, t.length() - 1)).split("\\|", -1);
			if (parts.length == 0) continue;
			String candidate = parts[0].trim();
			if (candidate.isBlank() || candidate.equalsIgnoreCase("Node (ENTITY_NAME)")) continue;

			// Remove bold markers if any
			candidate = candidate.replace("**", "").replace("*", "").trim();

			if (!candidate.isBlank()) {
				nodeIds.add(candidate);
				if (nodeIds.size() >= limit) break;
			}
		}

		// If no perfect header table was found, attempt heuristic extraction from any markdown table row.
		if (nodeIds.isEmpty()) {
			for (String line : lines) {
				String t = line.trim();
				if (!t.startsWith("|") || t.contains("---")) continue;
				String[] parts = t.substring(1, Math.max(0, t.length() - 1)).split("\\|", -1);
				if (parts.length < 2) continue;
				String candidate = parts[0].trim().replace("**", "").replace("*", "");
				if (candidate.isBlank()) continue;
				String cLower = candidate.toLowerCase();
				// Typical node identifiers in this project contain '-' or '_' and are not the literal "Node" header text.
				if (cLower.contains("node") || candidate.equalsIgnoreCase("Node (ENTITY_NAME)")) continue;
				if (candidate.contains("-") || candidate.contains("_")) {
					nodeIds.add(candidate);
					if (nodeIds.size() >= limit) break;
				}
			}
		}

		// Fallback: pull backtick-quoted tokens
		if (nodeIds.isEmpty()) {
			Pattern p = Pattern.compile("`([^`]+)`");
			Matcher m = p.matcher(analysisFindings);
			while (m.find() && nodeIds.size() < limit) {
				String candidate = m.group(1).trim();
				if (!candidate.isBlank()) nodeIds.add(candidate);
			}
		}

		return List.copyOf(nodeIds);
	}

	private static String formatEvidence(RcaResult result) {
		StringBuilder sb = new StringBuilder();
		sb.append("### JanusGraph Evidence Snapshot\n\n");
		if (result == null || result.groups() == null || result.groups().isEmpty()) {
			sb.append("_No topology groups resolved from JanusGraph._\n");
			if (result != null && result.executionNote() != null && !result.executionNote().isBlank()) {
				sb.append("_Note:_ ").append(result.executionNote()).append("\n");
			}
			return sb.toString();
		}

		for (int i = 0; i < result.groups().size(); i++) {
			RcaGroup group = result.groups().get(i);
			RcaNode root = group.rootCause();

			log.info("[JanusGraph][Evidence][ReportFormatter] Group #{} rootCauseId='{}' name='{}' hierarchyLevel='{}' alarmCount={} cascadedCount={} blastRadiusCount={}",
					i + 1,
					root.nodeId(),
					root.name(),
					root.hierarchyLevel(),
					root.alarmCount(),
					group.cascadedNodes().size(),
					group.blastRadius().size());

			sb.append("#### Group ").append(i + 1).append("\n");
			sb.append("- **Root cause:** `").append(root.name()).append("` (ID: `").append(root.nodeId()).append("`)\n");
			sb.append("- **Hierarchy level:** ").append(root.hierarchyLevel())
					.append(" | **Vendor:** ").append(root.vendor())
					.append(" | **alarmCount:** ").append(root.alarmCount()).append("\n");
			sb.append("- **Cascaded nodes:** ").append(group.cascadedNodes().size()).append("\n");
			sb.append("- **Blast radius nodes:** ").append(group.blastRadius().size()).append("\n\n");
		}

		return sb.toString();
	}

	private static String evidenceSample(String text, int maxChars) {
		if (text == null) return "";
		String s = text.replaceAll("\\s+", " ").trim();
		if (s.length() <= maxChars) return s;
		return s.substring(0, maxChars) + "...";
	}

	private static String evidenceSample(List<String> nodeIds, int max) {
		if (nodeIds == null || nodeIds.isEmpty()) return "";
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (String id : nodeIds) {
			if (i++ >= max) break;
			if (sb.length() > 0) sb.append(",");
			sb.append(id);
		}
		return sb.toString();
	}

	public static Builder builder(ChatClient.Builder chatClientBuilder) {
		return new Builder(chatClientBuilder);
	}

	public static class Builder {

		private static final String DEFAULT_SYSTEM_PROMPT = """
				You are a specialist NOC report formatter.

				You receive analysis findings from a NOC investigation (alarm data, KPI data,
				network intelligence findings) and produce a polished, professionally structured
				report appropriate for the requested report type and audience.

				Rules:
				- Use only the findings provided — never invent data
				- Match the structure and depth to the report type (executive vs. technical)
				- Lead with the most critical or actionable information
				- Keep language clear, direct, and jargon-appropriate for network operations
				- End with concrete recommended actions with owners and timelines
				""";

		private final ChatClient.Builder chatClientBuilder;
		private GraphTopologyService graphTopologyService;

		private Builder(ChatClient.Builder chatClientBuilder) {
			Assert.notNull(chatClientBuilder, "chatClientBuilder must not be null");
			this.chatClientBuilder = chatClientBuilder;
		}

		public Builder graphTopologyService(GraphTopologyService graphTopologyService) {
			this.graphTopologyService = graphTopologyService;
			return this;
		}

		public ReportFormatterTool build() {
			return new ReportFormatterTool(this.chatClientBuilder.clone(), DEFAULT_SYSTEM_PROMPT, this.graphTopologyService);
		}

	}

}
