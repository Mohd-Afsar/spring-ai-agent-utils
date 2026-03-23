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
import org.springframework.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

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

	private final GraphTopologyService graphTopologyService;

	private final NetworkIntelligenceTool networkIntelligenceTool;

	/**
	 * Cap the number of distinct alarming nodes we send into JanusGraph.
	 * Too many nodes makes RCA traversals expensive.
	 */
	private static final int MAX_ALARM_NODEIDS_FOR_TOPOLOGY = 50;

	private AlarmAnalystTool(ChatClient.Builder builder, GraphTopologyService graphTopologyService,
			NetworkIntelligenceTool networkIntelligenceTool) {
		this.chatClient = builder.defaultSystem(NOC_SYSTEM_PROMPT).build();
		this.graphTopologyService = graphTopologyService;
		this.networkIntelligenceTool = networkIntelligenceTool;
	}

	// @formatter:off
	@Tool(name = "AlarmAnalyst", description = """
			Senior NOC-style alarm + topology correlation. Uses telecom hierarchy rules (system prompt).

			Call AFTER DbQuery/DbSample on ALARM. Pass optional topology block from NetworkTopologyRca when available.
			Output: rootCause, impactAnalysis, alarmCorrelation, faultClassification, recommendedActions, confidence, then summary.
			""")
	public String analyzeAlarms( // @formatter:on
			@ToolParam(description = "Raw alarm data as fetched from the database (markdown table format)") String rawAlarmData,
			@ToolParam(description = "Investigation context: scope, time window, region, user goal") String investigationContext,
			@ToolParam(description = "Optional. JanusGraph / inventory topology markdown from NetworkTopologyRca (hierarchy + OSPF adjacency). Leave empty if not yet fetched.", required = false) String topologyHierarchyContext) {

		String topo = topologyHierarchyContext == null ? "" : topologyHierarchyContext.trim();
		log.info("[AlarmAnalyst] analyzeAlarms called. graphTopologyServicePresent={} evidenceAlreadyCalled={} rawAlarmDataChars={} topologyContextChars={}",
				this.graphTopologyService != null,
				AgentConsole.isJanusGraphEvidenceCalled(),
				rawAlarmData == null ? 0 : rawAlarmData.length(),
				topo.length());
		System.out.printf("[AlarmAnalyst] analyzeAlarms graphTopologyServicePresent=%s evidenceAlreadyCalled=%s rawChars=%d topologyChars=%d%n",
				this.graphTopologyService != null,
				AgentConsole.isJanusGraphEvidenceCalled(),
				rawAlarmData == null ? 0 : rawAlarmData.length(),
				topo.length());

		String prompt = String.format("""
				### investigationContext
				%s

				### topologyHierarchyDefinition
				(Static layers and rules are in your system instructions — TOPOLOGY LAYERS and HIERARCHY RELATIONSHIP RULES.)

				### topologyInstanceOrGraphEvidence
				%s

				### rawAlarmData
				%s

				Follow the system instructions. First emit the **STRICT OUTPUT FORMAT** block (rootCause through confidence), then a short **Human-readable summary** for handoff.
				Do not paste the full raw table again.
				""",
				investigationContext != null ? investigationContext : "",
				topo.isEmpty() ? "_No topology / JanusGraph block supplied in this request._" : topo,
				rawAlarmData != null ? rawAlarmData : "");

		AgentConsole.toolStarted("AlarmAnalyst");
		try {
			String alarmAnalysis = this.chatClient.prompt(prompt).call().content();

			// Deterministic enrichment: when JanusGraph is enabled, attach
			// hierarchy/graph evidence during alarm analysis using the same
			// distinct node IDs derived from the fetched raw alarm rows.
			if (this.graphTopologyService != null && !AgentConsole.isJanusGraphEvidenceCalled()) {
				List<String> nodeIds = extractDistinctNodeIds(rawAlarmData, MAX_ALARM_NODEIDS_FOR_TOPOLOGY);
				log.info("[AlarmAnalyst] Extracted alarming nodeIds for JanusGraph: count={} sample={}",
						nodeIds.size(),
						nodeIds.stream().limit(10).reduce((a, b) -> a + "," + b).orElse(""));
				System.out.println(String.format("[AlarmAnalyst] Extracted alarming nodeIds for JanusGraph: count=%d sample=%s",
						nodeIds.size(),
						nodeIds.stream().limit(10).reduce((a, b) -> a + "," + b).orElse("")));
				if (!nodeIds.isEmpty()) {
					AgentConsole.markJanusGraphEvidenceCalled();
					RcaResult rcaResult = this.graphTopologyService.analyzeRootCause(nodeIds);
					String topologyEvidence = formatEvidence(rcaResult);
					log.info("[AlarmAnalyst] Returning JanusGraph hierarchy evidence. groups={} evidenceChars={}",
							(rcaResult.groups() == null ? 0 : rcaResult.groups().size()),
							(topologyEvidence == null ? 0 : topologyEvidence.length()));

					if (this.networkIntelligenceTool != null) {
						String networkIntelligence = this.networkIntelligenceTool.analyzeNetworkIntelligence(
								topologyEvidence,
								investigationContext,
								alarmAnalysis);
						// Log a short sample of what we pass to NetworkIntelligence and what we return.
						log.info("[AlarmAnalyst] JanusGraph evidence sample: {}",
								evidenceSample(topologyEvidence, 400));
						return alarmAnalysis + "\n\n---\n\n" + topologyEvidence + "\n\n---\n\n" + networkIntelligence;
					}

					log.info("[AlarmAnalyst] JanusGraph evidence sample: {}", evidenceSample(topologyEvidence, 400));
					return alarmAnalysis + "\n\n---\n\n" + topologyEvidence;
				}
			}

			return alarmAnalysis;
		}
		finally {
			AgentConsole.toolFinished();
		}
	}

	/**
	 * Extract distinct node IDs from DbQuery markdown table output.
	 *
	 * <p>We detect the relevant column by inspecting the header row and looking
	 * for {@code nodeId} / {@code nodeName}.
	 */
	private static List<String> extractDistinctNodeIds(String rawAlarmData, int limit) {
		if (!StringUtils.hasText(rawAlarmData)) {
			return List.of();
		}

		String[] lines = rawAlarmData.split("\\r?\\n");
		List<String> tableLines = new ArrayList<>();
		for (String line : lines) {
			String t = line.trim();
			if (t.startsWith("|") && t.endsWith("|")) {
				tableLines.add(t);
			}
		}
		if (tableLines.size() < 3) return List.of();

		List<String> headerCells = splitMarkdownRow(tableLines.get(0));
		int nodeIdIdx = findHeaderIndex(headerCells, "nodeId");
		if (nodeIdIdx < 0) nodeIdIdx = findHeaderIndex(headerCells, "node_id");
		if (nodeIdIdx < 0) nodeIdIdx = findHeaderIndex(headerCells, "nodeName");
		if (nodeIdIdx < 0) nodeIdIdx = findHeaderIndex(headerCells, "node_name");
		if (nodeIdIdx < 0) {
			for (int i = 0; i < headerCells.size(); i++) {
				if (headerCells.get(i).toLowerCase(Locale.ROOT).contains("node")) {
					nodeIdIdx = i;
					break;
				}
			}
		}
		if (nodeIdIdx < 0) return List.of();

		Set<String> distinct = new LinkedHashSet<>();
		for (int li = 2; li < tableLines.size(); li++) {
			String row = tableLines.get(li);
			if (row.contains("---")) continue;
			List<String> cells = splitMarkdownRow(row);
			if (nodeIdIdx >= cells.size()) continue;
			String id = cells.get(nodeIdIdx).trim();
			if (!StringUtils.hasText(id) || "NULL".equalsIgnoreCase(id)) continue;
			distinct.add(id);
			if (distinct.size() >= limit) break;
		}

		return List.copyOf(distinct);
	}

	private static int findHeaderIndex(List<String> headers, String wanted) {
		String w = wanted.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
		for (int i = 0; i < headers.size(); i++) {
			String h = headers.get(i).toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
			if (h.equals(w) || h.contains(w)) {
				return i;
			}
		}
		return -1;
	}

	private static List<String> splitMarkdownRow(String row) {
		if (!StringUtils.hasText(row)) return List.of();
		String trimmed = row.trim();
		if (!trimmed.startsWith("|") || !trimmed.endsWith("|")) return List.of();
		trimmed = trimmed.substring(1, trimmed.length() - 1);
		String[] parts = trimmed.split("\\|", -1);
		List<String> cells = new ArrayList<>(parts.length);
		for (String part : parts) {
			cells.add(part.trim());
		}
		return cells;
	}

	private static String formatEvidence(RcaResult result) {
		StringBuilder sb = new StringBuilder();
		sb.append("### JanusGraph Evidence Snapshot\n\n");
		if (result.groups() == null || result.groups().isEmpty()) {
			sb.append("_No topology groups resolved from JanusGraph._\n");
			if (result.executionNote() != null && !result.executionNote().isBlank()) {
				sb.append("_Note:_ ").append(result.executionNote()).append("\n");
			}
			return sb.toString();
		}

		sb.append("**Fault groups detected:** ").append(result.groups().size()).append("\n\n");
		for (int i = 0; i < result.groups().size(); i++) {
			RcaGroup group = result.groups().get(i);
			RcaNode root = group.rootCause();

			// Final step: log the exact hierarchy facts we are embedding in the evidence block.
			log.info("[JanusGraph][Evidence] Group #{} rootCauseId='{}' name='{}' hierarchyLevel='{}' alarmCount={} cascadedCount={} blastRadiusCount={}",
					(i + 1),
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

	private static String evidenceSample(String evidence, int maxChars) {
		if (evidence == null) return "";
		String s = evidence.replaceAll("\\s+", " ").trim();
		if (s.length() <= maxChars) return s;
		return s.substring(0, maxChars) + "...";
	}

	public static Builder builder(ChatClient.Builder chatClientBuilder) {
		return new Builder(chatClientBuilder);
	}

	public static class Builder {

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

		public AlarmAnalystTool build() {
			NetworkIntelligenceTool networkIntelligenceTool = (this.graphTopologyService != null)
					? NetworkIntelligenceTool.builder(this.chatClientBuilder.clone()).build()
					: null;
			return new AlarmAnalystTool(this.chatClientBuilder.clone(), this.graphTopologyService, networkIntelligenceTool);
		}

	}

}
