package org.springaicommunity.nova.tools;

import org.springaicommunity.nova.AgentConsole;
import org.springaicommunity.nova.graph.AlarmTopologySeedResolver;
import org.springaicommunity.nova.graph.GraphTopologyService;
import org.springaicommunity.nova.graph.TopologyNeNames;
import org.springframework.ai.chat.client.ChatClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

import javax.sql.DataSource;

/**
 * JanusGraph-backed topology for RCA: resolves alarm-row fields to inventory {@code NE_NAME} / {@code nodeId},
 * then emits parent/child/OSPF-adjacency structure for the LLM to combine with alarm semantics.
 */
public class NetworkTopologyRcaTool {

	private static final Logger log = LoggerFactory.getLogger(NetworkTopologyRcaTool.class);

	private final GraphTopologyService graphTopologyService;
	private final NetworkIntelligenceTool networkIntelligenceTool;
	private final DataSource dataSource;
	private final int maxAlarmingNodeIds;
	private final int maxChildrenPerNodeInReport;
	private final int maxLinkNeighborsPerNode;
	private final int maxTopologyCharsForNetworkIntel;

	public NetworkTopologyRcaTool(GraphTopologyService graphTopologyService, NetworkIntelligenceTool networkIntelligenceTool,
			DataSource dataSource,
			int maxAlarmingNodeIds, int maxChildrenPerNodeInReport, int maxLinkNeighborsPerNode,
			int maxTopologyCharsForNetworkIntel) {
		this.graphTopologyService = graphTopologyService;
		this.networkIntelligenceTool = networkIntelligenceTool;
		this.dataSource = dataSource;
		this.maxAlarmingNodeIds = Math.max(1, maxAlarmingNodeIds);
		this.maxChildrenPerNodeInReport = Math.max(1, maxChildrenPerNodeInReport);
		this.maxLinkNeighborsPerNode = Math.max(1, maxLinkNeighborsPerNode);
		this.maxTopologyCharsForNetworkIntel = maxTopologyCharsForNetworkIntel;
	}

	// @formatter:off
	@Tool(name = "NetworkTopologyRca", description = """
			JanusGraph topology for RCA: parent/child (PARENT_OF) and OSPF adjacency (via interfaces).

			Pass comma-separated NE names / nodeIds from alarms. Prefer calling after DbQuery on ALARM.
			Output is structured so the orchestrator can correlate alarms with shared ancestors or transport peers.
			""")
	public String analyzeTopologyRootCause( // @formatter:on
			@ToolParam(description = "Comma-separated alarming network node IDs from the alarm database (e.g. ENTITY_NAME / NE_NAME)") String alarmingNodeIds,
			@ToolParam(description = "Investigation context: time window, region, alarm summary.") String investigationContext) {

		List<String> nodeIds = Arrays.stream(alarmingNodeIds.split(","))
				.map(String::trim)
				.filter(s -> !s.isBlank())
				.collect(Collectors.toList());

		int mergedSize = nodeIds.size();
		if (nodeIds.size() > maxAlarmingNodeIds) {
			nodeIds = new ArrayList<>(nodeIds.subList(0, maxAlarmingNodeIds));
			log.warn("[NetworkTopologyRca] Truncating alarming node IDs. originalCount={} truncatedCount={} (cap nova.topology.max-alarming-nodes={})",
					mergedSize, nodeIds.size(), maxAlarmingNodeIds);
		}
		int afterCapCount = nodeIds.size();

		logInvocation(nodeIds, investigationContext);
		if (nodeIds.isEmpty()) {
			return "[NetworkTopologyRca] No valid node IDs provided.";
		}

		return runTopologyPipeline(nodeIds, mergedSize, afterCapCount, investigationContext, List.of(), List.of());
	}

	/**
	 * Deterministic / rich path: scan alarm query markdown for topology-related columns, resolve to
	 * {@code NETWORK_ELEMENT.NE_NAME}, merge with literal values, then build the same graph report.
	 */
	public String analyzeTopologyForAlarmMarkdown(String alarmQueryMarkdown, String investigationContext) {
		List<String> seeds = AlarmTopologySeedResolver.extractSeedsFromAlarmMarkdown(alarmQueryMarkdown);
		List<String> resolved = List.of();
		if (dataSource != null && !seeds.isEmpty()) {
			try {
				resolved = AlarmTopologySeedResolver.resolveSeedsToNeNames(dataSource, seeds, maxAlarmingNodeIds * 2);
			}
			catch (SQLException e) {
				log.warn("[NetworkTopologyRca] MySQL seed resolution failed: {}", e.getMessage());
			}
		}
		LinkedHashSet<String> merged = new LinkedHashSet<>(resolved);
		for (String s : seeds) {
			if (merged.size() >= maxAlarmingNodeIds) {
				break;
			}
			if (s != null && !s.isBlank()) {
				merged.add(s.trim());
			}
		}
		List<String> nodeIds = new ArrayList<>(merged);
		int mergedSize = nodeIds.size();
		if (nodeIds.size() > maxAlarmingNodeIds) {
			nodeIds = new ArrayList<>(nodeIds.subList(0, maxAlarmingNodeIds));
		}
		int afterCapCount = nodeIds.size();
		log.info("[NetworkTopologyRca] Alarm-markdown path: seeds={} resolvedToNeName={} graphLookups={}",
				seeds.size(), resolved.size(), afterCapCount);
		if (nodeIds.isEmpty()) {
			return "[NetworkTopologyRca] No topology seeds extracted from alarm markdown (need | table| columns like ENTITY_NAME, HOSTNAME, INTERFACE, IP, …).";
		}
		return runTopologyPipeline(nodeIds, mergedSize, afterCapCount, investigationContext, seeds, resolved);
	}

	private void logInvocation(List<String> nodeIds, String investigationContext) {
		List<String> nodeIdSample = nodeIds.stream().limit(15).toList();
		String ctx = investigationContext != null ? investigationContext : "";
		if (ctx.length() > 160) {
			ctx = ctx.substring(0, 160) + "...";
		}
		log.info("[NetworkTopologyRca] Invoked. nodeIdsCount={} sampleNodeIds={} context='{}'",
				nodeIds.size(), nodeIdSample, ctx);
	}

	private static String buildLlmRcaProlog(List<String> seeds, List<String> resolved, List<String> nodeIdsUsed) {
		StringBuilder sb = new StringBuilder();
		sb.append("## RCA — how to use topology + alarms\n\n");
		sb.append("1. Cross-check **alarm entity / object / interface / IP** with the sections below.\n");
		sb.append("2. **Shared upstream** (same `PARENT_OF` chain prefix) across several alarming NEs → common hub, controller, or aggregation fault.\n");
		sb.append("3. **Adjacent equipment (OSPF via interfaces)** → likely transport or area issue affecting peers.\n");
		sb.append("4. Empty **direct CONNECTED_TO** on routers is normal: OSPF edges attach to **interface** vertices.\n\n");
		sb.append("### Alarm rows → inventory\n\n");
		sb.append("- **Seeds** parsed from alarm markdown columns: ").append(seeds.size()).append(" distinct.\n");
		sb.append("- **Resolved** to `NETWORK_ELEMENT.NE_NAME` (Janus `nodeId`): ").append(resolved.size()).append(".\n");
		sb.append("- **NEs queried in graph**: ").append(nodeIdsUsed.size()).append(".\n");
		int lim = Math.min(20, seeds.size());
		if (lim > 0) {
			sb.append("- Sample seeds: ");
			sb.append(seeds.stream().limit(lim).map(s -> "`" + s + "`").collect(Collectors.joining(", ")));
			if (seeds.size() > lim) {
				sb.append(" … (+").append(seeds.size() - lim).append(" more)");
			}
			sb.append("\n");
		}
		lim = Math.min(20, resolved.size());
		if (lim > 0) {
			sb.append("- Sample resolved NE_NAME: ");
			sb.append(resolved.stream().limit(lim).map(s -> "`" + s + "`").collect(Collectors.joining(", ")));
			if (resolved.size() > lim) {
				sb.append(" … (+").append(resolved.size() - lim).append(" more)");
			}
			sb.append("\n");
		}
		sb.append("\n");
		return sb.toString();
	}

	private String runTopologyPipeline(List<String> nodeIds, int mergedSize, int afterCapCount,
			String investigationContext, List<String> prologSeeds, List<String> prologResolved) {
		AgentConsole.toolStarted("NetworkTopologyRca");
		try {
			AgentConsole.markJanusGraphEvidenceCalled();
			int beforeEquipmentRollup = nodeIds.size();
			List<String> equipmentIds = TopologyNeNames.distinctParentEquipment(nodeIds, maxAlarmingNodeIds);
			if (!equipmentIds.isEmpty()) {
				if (equipmentIds.size() < beforeEquipmentRollup) {
					log.info("[NetworkTopologyRca] Equipment rollup for report: {} -> {} NE_NAME(s)",
							beforeEquipmentRollup, equipmentIds.size());
				}
				nodeIds = new ArrayList<>(equipmentIds);
			}
			String llmProlog = null;
			if (prologSeeds != null && !prologSeeds.isEmpty()) {
				llmProlog = buildLlmRcaProlog(prologSeeds, prologResolved != null ? prologResolved : List.of(), nodeIds);
			}
			String structure = graphTopologyService.formatInventoryTopologyReport(
					nodeIds, maxChildrenPerNodeInReport, maxLinkNeighborsPerNode);
			String topologyReport = prependContext(structure, investigationContext, nodeIds, mergedSize, afterCapCount,
					llmProlog);

			if (this.networkIntelligenceTool != null) {
				boolean niSizeOk = maxTopologyCharsForNetworkIntel <= 0
						|| topologyReport.length() <= maxTopologyCharsForNetworkIntel;
				if (!niSizeOk) {
					log.warn("[NetworkTopologyRca] Skipping NetworkIntelligence due to large topology report chars={} (cap={})",
							topologyReport.length(), maxTopologyCharsForNetworkIntel);
					return topologyReport
							+ "\n\n---\n\n"
							+ "_NetworkIntelligence skipped (payload too large)._";
				}
				String networkIntelligence = this.networkIntelligenceTool.analyzeNetworkIntelligence(
						topologyReport,
						investigationContext,
						investigationContext);
				return topologyReport + "\n\n---\n\n" + networkIntelligence;
			}
			return topologyReport;
		}
		catch (Exception e) {
			log.error("[NetworkTopologyRca] Failed. context={}", investigationContext, e);
			return "[NetworkTopologyRca error: " + e.getMessage() + ". Check JanusGraph and janusgraph.enabled.]";
		}
		finally {
			AgentConsole.toolFinished();
		}
	}

	private String prependContext(String body, String investigationContext, List<String> nodeIds, int mergedSize,
			int afterCapCount, String llmProlog) {
		String ctx = investigationContext != null ? investigationContext : "";
		StringBuilder head = new StringBuilder();
		if (llmProlog != null && !llmProlog.isBlank()) {
			head.append(llmProlog);
		}
		head.append("**Investigation context:** ").append(ctx.isBlank() ? "_(none)_" : ctx).append("\n\n");
		head.append("**Topology report scope:** ").append(nodeIds.size()).append(" unique equipment NE(s)");
		if (afterCapCount > nodeIds.size()) {
			head.append(" _(rolled up from ").append(afterCapCount).append(" inventory `NE_NAME` rows)_");
		}
		if (mergedSize > afterCapCount) {
			head.append(" _(cap: first ").append(afterCapCount).append(" of ").append(mergedSize).append(" resolved)_");
		}
		head.append(" | sample: ");
		head.append(String.join(", ", nodeIds.stream().limit(12).toList()));
		if (nodeIds.size() > 12) {
			head.append(" … (+").append(nodeIds.size() - 12).append(" more)");
		}
		head.append("\n\n");
		head.append(body);
		head.append("\n\n---\n");
		head.append("_Use **PmDataFetch** on hot NEs for KPIs._\n");
		return head.toString();
	}

	public static NetworkTopologyRcaTool of(GraphTopologyService service, ChatClient.Builder chatClientBuilder) {
		return of(service, chatClientBuilder, 120, 30, 25, 100_000, null);
	}

	public static NetworkTopologyRcaTool of(GraphTopologyService service, ChatClient.Builder chatClientBuilder,
			int maxAlarmingNodeIds, int maxChildrenPerNode, int maxLinkNeighbors, int maxTopologyCharsForNetworkIntel) {
		return of(service, chatClientBuilder, maxAlarmingNodeIds, maxChildrenPerNode, maxLinkNeighbors,
				maxTopologyCharsForNetworkIntel, null);
	}

	public static NetworkTopologyRcaTool of(GraphTopologyService service, ChatClient.Builder chatClientBuilder,
			int maxAlarmingNodeIds, int maxChildrenPerNode, int maxLinkNeighbors, int maxTopologyCharsForNetworkIntel,
			DataSource dataSource) {
		NetworkIntelligenceTool networkIntelligence = NetworkIntelligenceTool.builder(chatClientBuilder).build();
		return new NetworkTopologyRcaTool(service, networkIntelligence, dataSource, maxAlarmingNodeIds, maxChildrenPerNode,
				maxLinkNeighbors, maxTopologyCharsForNetworkIntel);
	}

}
