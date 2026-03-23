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

	/** First N characters of JanusGraph-derived payloads logged at INFO for operator visibility. */
	private static final int JANUS_TO_LLM_LOG_PREVIEW_CHARS = 2500;

	private final GraphTopologyService graphTopologyService;
	private final NetworkIntelligenceTool networkIntelligenceTool;
	private final DataSource dataSource;
	private final int maxAlarmingNodeIds;
	private final int maxChildrenPerNodeInReport;
	private final int maxLinkNeighborsPerNode;
	private final int maxTopologyCharsForNetworkIntel;
	/**
	 * When true, {@code ENTITY_NAME} values that look like {@code Router_xe-0/0/0_…} collapse to the parent
	 * equipment NE for one graph section per router. When false, every distinct alarming {@code NE_NAME} is queried
	 * (e.g. all 345 open-alarm entities).
	 */
	private final boolean rollUpInterfaceAlarmsToParentEquipment;

	public NetworkTopologyRcaTool(GraphTopologyService graphTopologyService, NetworkIntelligenceTool networkIntelligenceTool,
			DataSource dataSource,
			int maxAlarmingNodeIds, int maxChildrenPerNodeInReport, int maxLinkNeighborsPerNode,
			int maxTopologyCharsForNetworkIntel, boolean rollUpInterfaceAlarmsToParentEquipment) {
		this.graphTopologyService = graphTopologyService;
		this.networkIntelligenceTool = networkIntelligenceTool;
		this.dataSource = dataSource;
		// 0 or negative = unlimited (no truncation; Gremlin traversals omit .limit()).
		this.maxAlarmingNodeIds = maxAlarmingNodeIds <= 0 ? Integer.MAX_VALUE : maxAlarmingNodeIds;
		this.maxChildrenPerNodeInReport = maxChildrenPerNodeInReport <= 0 ? Integer.MAX_VALUE : maxChildrenPerNodeInReport;
		this.maxLinkNeighborsPerNode = maxLinkNeighborsPerNode <= 0 ? Integer.MAX_VALUE : maxLinkNeighborsPerNode;
		this.maxTopologyCharsForNetworkIntel = maxTopologyCharsForNetworkIntel;
		this.rollUpInterfaceAlarmsToParentEquipment = rollUpInterfaceAlarmsToParentEquipment;
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
		List<String> inputSeeds = List.copyOf(nodeIds);
		List<String> resolvedNodeIds = graphTopologyService.resolveToNodeIds(inputSeeds, maxAlarmingNodeIds * 2);
		if (!resolvedNodeIds.isEmpty()) {
			nodeIds = new ArrayList<>(resolvedNodeIds);
		}
		log.info("[NetworkTopologyRca] Dynamic seed resolution: inputSeeds={} resolvedNodeIds={} sampleResolved={}",
				inputSeeds.size(), nodeIds.size(), nodeIds.stream().limit(10).toList());

		int mergedSize = nodeIds.size();
		if (maxAlarmingNodeIds != Integer.MAX_VALUE && nodeIds.size() > maxAlarmingNodeIds) {
			nodeIds = new ArrayList<>(nodeIds.subList(0, maxAlarmingNodeIds));
			log.warn("[NetworkTopologyRca] Truncating alarming node IDs. originalCount={} truncatedCount={} (cap nova.topology.max-alarming-nodes={})",
					mergedSize, nodeIds.size(), maxAlarmingNodeIds);
		}
		int afterCapCount = nodeIds.size();

		logInvocation(nodeIds, investigationContext);
		if (nodeIds.isEmpty()) {
			return "[NetworkTopologyRca] No valid node IDs provided.";
		}

		return runTopologyPipeline(nodeIds, mergedSize, afterCapCount, investigationContext, List.of(), List.of(), true,
				false, 0);
	}

	/**
	 * Deterministic / rich path: scan alarm query markdown for topology-related columns, resolve to
	 * {@code NETWORK_ELEMENT.NE_NAME}, merge with literal values, then build the same graph report.
	 */
	public String analyzeTopologyForAlarmMarkdown(String alarmQueryMarkdown, String investigationContext) {
		return analyzeTopologyForAlarmMarkdown(alarmQueryMarkdown, investigationContext, true);
	}

	/**
	 * @param runNetworkIntelligence when {@code false}, skip the extra NetworkIntelligence LLM pass (saves tokens).
	 */
	public String analyzeTopologyForAlarmMarkdown(String alarmQueryMarkdown, String investigationContext,
			boolean runNetworkIntelligence) {
		int seedExtractCap = maxAlarmingNodeIds == Integer.MAX_VALUE ? Integer.MAX_VALUE : maxAlarmingNodeIds * 8;
		int resolveCap = maxAlarmingNodeIds == Integer.MAX_VALUE ? Integer.MAX_VALUE : maxAlarmingNodeIds * 4;
		int graphCap = maxAlarmingNodeIds == Integer.MAX_VALUE ? Integer.MAX_VALUE : maxAlarmingNodeIds;
		List<String> seeds = AlarmTopologySeedResolver.extractSeedsFromAlarmMarkdown(alarmQueryMarkdown, seedExtractCap);
		List<String> resolved = List.of();
		if (dataSource != null && !seeds.isEmpty()) {
			try {
				resolved = AlarmTopologySeedResolver.resolveSeedsToNeNames(dataSource, seeds, resolveCap);
				if (resolved.size() >= resolveCap) {
					log.warn("[NetworkTopologyRca] MySQL NE_NAME resolution hit cap {} (distinct seeds={}). "
							+ "Raise nova.topology.max-alarming-nodes or lower alarm row volume; cap scales with table rows.",
							resolveCap, seeds.size());
				}
			}
			catch (SQLException e) {
				log.warn("[NetworkTopologyRca] MySQL seed resolution failed: {}", e.getMessage());
			}
		}
		LinkedHashSet<String> full = new LinkedHashSet<>();
		for (String r : resolved) {
			if (r != null && !r.isBlank()) {
				full.add(r.trim());
			}
		}
		for (String s : seeds) {
			if (s != null && !s.isBlank()) {
				full.add(s.trim());
			}
		}
		int fullDistinct = full.size();
		List<String> nodeIds = new ArrayList<>();
		for (String x : full) {
			if (nodeIds.size() >= graphCap) {
				break;
			}
			nodeIds.add(x);
		}
		int neAtEntry = nodeIds.size();
		boolean hitResolveCap = resolved.size() >= resolveCap;
		log.info("[NetworkTopologyRca] Alarm-markdown path: seeds={} resolvedToNeName={} distinctMerged={} graphLookups={} resolveCap={}",
				seeds.size(), resolved.size(), fullDistinct, neAtEntry, resolveCap);
		if (nodeIds.isEmpty()) {
			return "[NetworkTopologyRca] No topology seeds extracted from alarm markdown (need | table| columns like ENTITY_NAME, HOSTNAME, INTERFACE, IP, …).";
		}
		return runTopologyPipeline(nodeIds, fullDistinct, neAtEntry, investigationContext, seeds, resolved,
				runNetworkIntelligence, hitResolveCap, resolveCap);
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

	private static String buildLlmRcaProlog(List<String> seeds, List<String> resolved, List<String> nodeIdsUsed,
			boolean hitMysqlResolveCap, int mysqlResolveCap) {
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
		if (hitMysqlResolveCap && mysqlResolveCap > 0) {
			sb.append("- **Note:** MySQL resolution returned at least **").append(mysqlResolveCap)
					.append("** distinct NE_NAME (cap). Some seeds may be unmapped in the graph sections.\n");
		}
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

	private String runTopologyPipeline(List<String> nodeIds, int distinctOrPreTruncBound, int neCountAtPipelineEntry,
			String investigationContext, List<String> prologSeeds, List<String> prologResolved,
			boolean runNetworkIntelligence, boolean hitMysqlResolveCap, int mysqlResolveCap) {
		AgentConsole.toolStarted("NetworkTopologyRca");
		try {
			AgentConsole.markJanusGraphEvidenceCalled();
			int beforeEquipmentRollup = nodeIds.size();
			if (rollUpInterfaceAlarmsToParentEquipment) {
				int equipCap = maxAlarmingNodeIds == Integer.MAX_VALUE ? 0 : maxAlarmingNodeIds;
				List<String> equipmentIds = TopologyNeNames.distinctParentEquipment(nodeIds, equipCap);
				if (!equipmentIds.isEmpty()) {
					if (equipmentIds.size() < beforeEquipmentRollup) {
						log.info("[NetworkTopologyRca] Equipment rollup for report: {} -> {} NE_NAME(s)",
								beforeEquipmentRollup, equipmentIds.size());
					}
					nodeIds = new ArrayList<>(equipmentIds);
				}
			}
			else {
				log.info("[NetworkTopologyRca] Equipment rollup disabled — reporting {} distinct alarming NE_NAME(s) as-is",
						nodeIds.size());
			}
			String llmProlog = null;
			if (prologSeeds != null && !prologSeeds.isEmpty()) {
				llmProlog = buildLlmRcaProlog(prologSeeds, prologResolved != null ? prologResolved : List.of(), nodeIds,
						hitMysqlResolveCap, mysqlResolveCap);
			}
			String inventoryMarkdown = graphTopologyService.formatInventoryTopologyReport(
					nodeIds, maxChildrenPerNodeInReport, maxLinkNeighborsPerNode);
			String topologyJson = graphTopologyService.formatAlarmTopologySubgraphJson(
					nodeIds, maxChildrenPerNodeInReport, maxLinkNeighborsPerNode);
			String jsonFence = "\n\n### Alarm-focused topology (JSON)\n\n```json\n" + topologyJson + "\n```\n";
			String structure = inventoryMarkdown + jsonFence;
			String topologyReport = prependContext(structure, investigationContext, nodeIds, distinctOrPreTruncBound,
					neCountAtPipelineEntry, llmProlog, rollUpInterfaceAlarmsToParentEquipment);

			log.info("[JanusGraph→LLM] Topology sections — inventoryMarkdownChars={}, subgraphJsonChars={}, "
							+ "structureChars={}, fullToolPayloadChars={} (prolog/context/footer included)",
					inventoryMarkdown.length(), topologyJson.length(), structure.length(), topologyReport.length());
			log.info("[JanusGraph→LLM] Topology tool payload preview (first {} of {} chars):\n{}",
					Math.min(JANUS_TO_LLM_LOG_PREVIEW_CHARS, topologyReport.length()),
					topologyReport.length(),
					truncForLog(topologyReport, JANUS_TO_LLM_LOG_PREVIEW_CHARS));

			if (this.networkIntelligenceTool != null && runNetworkIntelligence) {
				boolean niSizeOk = maxTopologyCharsForNetworkIntel <= 0
						|| topologyReport.length() <= maxTopologyCharsForNetworkIntel;
				if (!niSizeOk) {
					log.warn("[NetworkTopologyRca] Skipping NetworkIntelligence due to large topology report chars={} (cap={})",
							topologyReport.length(), maxTopologyCharsForNetworkIntel);
					String skippedNi = topologyReport + "\n\n---\n\n"
							+ "_NetworkIntelligence skipped (payload too large)._";
					log.info("[JanusGraph→LLM] Final tool return (NI skipped) totalChars={}", skippedNi.length());
					log.info("[JanusGraph→LLM] Final tool return preview:\n{}", truncForLog(skippedNi, JANUS_TO_LLM_LOG_PREVIEW_CHARS));
					return skippedNi;
				}
				log.info("[JanusGraph→LLM] Passing topologyReport to NetworkIntelligence LLM — chars={}",
						topologyReport.length());
				String networkIntelligence = this.networkIntelligenceTool.analyzeNetworkIntelligence(
						topologyReport,
						investigationContext,
						investigationContext);
				String combined = topologyReport + "\n\n---\n\n" + networkIntelligence;
				log.info("[JanusGraph→LLM] Final tool return — totalChars={} (topologyChars={}, networkIntelligenceChars={})",
						combined.length(), topologyReport.length(), networkIntelligence.length());
				log.info("[JanusGraph→LLM] Final tool return preview (first {} of {} chars):\n{}",
						Math.min(JANUS_TO_LLM_LOG_PREVIEW_CHARS, combined.length()),
						combined.length(),
						truncForLog(combined, JANUS_TO_LLM_LOG_PREVIEW_CHARS));
				return combined;
			}
			if (!runNetworkIntelligence && this.networkIntelligenceTool != null) {
				return topologyReport + "\n\n---\n\n_NetworkIntelligence skipped on this path (runNetworkIntelligence=false)._";
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

	private static String truncForLog(String s, int maxChars) {
		if (s == null || s.isEmpty()) {
			return "";
		}
		if (s.length() <= maxChars) {
			return s;
		}
		return s.substring(0, maxChars) + "\n... [+" + (s.length() - maxChars) + " chars omitted]";
	}

	private String prependContext(String body, String investigationContext, List<String> nodeIds,
			int distinctOrPreTruncBound, int neCountAtPipelineEntry, String llmProlog, boolean rolledUpToEquipment) {
		String ctx = investigationContext != null ? investigationContext : "";
		StringBuilder head = new StringBuilder();
		if (llmProlog != null && !llmProlog.isBlank()) {
			head.append(llmProlog);
		}
		head.append("**Investigation context:** ").append(ctx.isBlank() ? "_(none)_" : ctx).append("\n\n");
		head.append("**Topology report scope:** ").append(nodeIds.size());
		head.append(rolledUpToEquipment ? " unique equipment NE(s)" : " unique alarming NE_NAME(s) (no parent rollup)");
		if (rolledUpToEquipment && neCountAtPipelineEntry > nodeIds.size()) {
			head.append(" _(equipment rollup: ").append(neCountAtPipelineEntry).append(" alarming NE/interface → ")
					.append(nodeIds.size()).append(" parent equipment NE(s))_");
		}
		if (distinctOrPreTruncBound > neCountAtPipelineEntry) {
			head.append(" _(graph NE list: first ").append(neCountAtPipelineEntry).append(" of ")
					.append(distinctOrPreTruncBound).append(" distinct merged seeds / NE_NAME — raise ")
					.append("`nova.topology.max-alarming-nodes` if you need a wider cap)_");
		}
		head.append(" | sample: ");
		head.append(String.join(", ", nodeIds.stream().limit(12).toList()));
		if (nodeIds.size() > 12) {
			head.append(" … (+").append(nodeIds.size() - 12).append(" more)");
		}
		head.append("\n\n");
		head.append(body);
		head.append("\n\n---\n");
		head.append("_**PmDataFetch** is only for optional KPI time-series — it is **not** called by this tool. ")
				.append("The text **PM** inside interface / circuit `neName` values is a **link description**, not Performance Management._\n");
		return head.toString();
	}

	public static NetworkTopologyRcaTool of(GraphTopologyService service, ChatClient.Builder chatClientBuilder) {
		return of(service, chatClientBuilder, 0, 0, 0, 100_000, null, true);
	}

	public static NetworkTopologyRcaTool of(GraphTopologyService service, ChatClient.Builder chatClientBuilder,
			int maxAlarmingNodeIds, int maxChildrenPerNode, int maxLinkNeighbors, int maxTopologyCharsForNetworkIntel) {
		return of(service, chatClientBuilder, maxAlarmingNodeIds, maxChildrenPerNode, maxLinkNeighbors,
				maxTopologyCharsForNetworkIntel, null, true);
	}

	public static NetworkTopologyRcaTool of(GraphTopologyService service, ChatClient.Builder chatClientBuilder,
			int maxAlarmingNodeIds, int maxChildrenPerNode, int maxLinkNeighbors, int maxTopologyCharsForNetworkIntel,
			DataSource dataSource) {
		return of(service, chatClientBuilder, maxAlarmingNodeIds, maxChildrenPerNode, maxLinkNeighbors,
				maxTopologyCharsForNetworkIntel, dataSource, true);
	}

	public static NetworkTopologyRcaTool of(GraphTopologyService service, ChatClient.Builder chatClientBuilder,
			int maxAlarmingNodeIds, int maxChildrenPerNode, int maxLinkNeighbors, int maxTopologyCharsForNetworkIntel,
			DataSource dataSource, boolean rollUpInterfaceAlarmsToParentEquipment) {
		NetworkIntelligenceTool networkIntelligence = NetworkIntelligenceTool.builder(chatClientBuilder).build();
		return new NetworkTopologyRcaTool(service, networkIntelligence, dataSource, maxAlarmingNodeIds, maxChildrenPerNode,
				maxLinkNeighbors, maxTopologyCharsForNetworkIntel, rollUpInterfaceAlarmsToParentEquipment);
	}

}
