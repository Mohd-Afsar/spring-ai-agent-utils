package org.springaicommunity.nova.graph;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Core graph-based Root Cause Analysis engine.
 *
 * <h3>Algorithm overview</h3>
 * <ol>
 *   <li><b>Ancestor map</b> — for each alarming node, walk UP the PARENT_OF
 *       edge chain and collect every ancestor node ID.</li>
 *   <li><b>Frequency count</b> — count how many alarming nodes share each
 *       ancestor (i.e. how many alarm chains pass through it).</li>
 *   <li><b>Coverage score</b> — frequency ÷ total alarming nodes.
 *       An ancestor with 100 % coverage appears in every alarm chain.</li>
 *   <li><b>Most specific root</b> — among ancestors with equal (max) coverage,
 *       prefer the one that is deepest in the hierarchy (most specific),
 *       measured by counting its own ancestors.</li>
 *   <li><b>Threshold decision</b>
 *     <ul>
 *       <li>Coverage ≥ 85 % → single root cause (cascade or shared resource)</li>
 *       <li>50–84 % → partial cascade, report the dominant group + remainders</li>
 *       <li>&lt; 50 % → multiple independent root causes, cluster greedily</li>
 *     </ul>
 *   </li>
 *   <li><b>Pattern detection</b> — classify each group as
 *       SINGLE_CASCADE / SHARED_RESOURCE / ISOLATED_LEAF / LINK_FAILURE /
 *       PARTIAL_CASCADE / MULTIPLE_ROOT_CAUSES.</li>
 *   <li><b>Blast radius</b> — walk DOWN from the root cause to find all
 *       downstream nodes at risk.</li>
 *   <li><b>Confidence score</b> — combines coverage, direct alarm evidence on
 *       root, cascade size, and sibling alarm evidence.</li>
 * </ol>
 *
 * <p>All Gremlin calls are wrapped with try-catch so a single unreachable node
 * never aborts the whole RCA — it is logged and treated as having no ancestors.
 *
 * @author Spring AI Community
 */
@Service
@ConditionalOnBean(GraphTraversalSource.class)
public class GraphTopologyService {

	private static final Logger log = LoggerFactory.getLogger(GraphTopologyService.class);

	/** Avoid excessive logs when many nodes are analyzed in one request. */
	private static final int MAX_INFO_NODE_LOGS = 10;

	/** Ancestors with this coverage fraction or higher → single root cause. */
	private static final double SINGLE_ROOT_THRESHOLD = 0.85;

	/** Ancestors with this coverage fraction or higher → partial cascade (not independent). */
	private static final double PARTIAL_CASCADE_THRESHOLD = 0.50;

	/** Sibling-alarm ratio above this → SHARED_RESOURCE pattern. */
	private static final double SHARED_RESOURCE_SIBLING_RATIO = 0.60;

	/**
	 * {@code <= 0} = unlimited (use with care on large graphs).
	 * @see #capJsonSubgraphVertices()
	 */
	@Value("${nova.topology.max-json-subgraph-vertices:0}")
	private int maxJsonSubgraphVerticesProp;

	@Value("${nova.topology.max-json-descendants-per-seed:0}")
	private int maxJsonDescendantsPerSeedProp;

	@Value("${nova.topology.max-inventory-report-nodes:0}")
	private int maxInventoryReportNodesProp;

	/**
	 * When {@code PARENT_OF} equipment→interface edges are missing in Janus but interface vertices still use
	 * {@code nodeId} = {@code NE_NAME} with the usual {@code {equipment}_{interface…}} pattern, discover those
	 * interfaces via {@code TextP.startingWith(equipment + "_")} so OSPF {@code CONNECTED_TO} and neighbors appear.
	 */
	@Value("${nova.topology.prefix-fallback-for-interface-vertices:true}")
	private boolean prefixFallbackForInterfaceVertices;

	@Value("${nova.topology.prefix-fallback-interfaces-per-equipment:100}")
	private int prefixFallbackInterfacesPerEquipment;

	/**
	 * When {@code max-children-per-node} / {@code max-link-neighbors-per-node} are {@code 0} (unlimited), Gremlin
	 * fan-out per NE can take minutes (hundreds of interfaces × OSPF walks). These soft caps keep alarm reports fast.
	 * Set to {@code 0} for truly unlimited (slow on large routers).
	 */
	@Value("${nova.topology.soft-cap-children-per-node-when-unlimited:0}")
	private int softCapChildrenWhenUnlimited;

	@Value("${nova.topology.soft-cap-neighbors-per-node-when-unlimited:0}")
	private int softCapNeighborsWhenUnlimited;

	/** Per equipment NE: only first N interface rows get indented OSPF peer lines (each line = 1 Gremlin round-trip). 0 = all. */
	@Value("${nova.topology.inventory-max-interfaces-for-ospf-peer-lines:12}")
	private int inventoryMaxInterfacesForOspfPeerLines;

	/**
	 * Cap how many interface {@code nodeId}s feed the <b>batched</b> neighbor query (0 = use all listed children — still
	 * one Gremlin batch per chunk, not one query per interface).
	 */
	@Value("${nova.topology.inventory-max-interfaces-for-neighbor-walk:0}")
	private int inventoryMaxInterfacesForNeighborWalk;

	/**
	 * Fast mode for alarm-topology use-cases:
	 * only hierarchy and 1-hop neighbor equipment are rendered.
	 */
	@Value("${nova.topology.fast-hierarchy-neighbors-only:true}")
	private boolean fastHierarchyNeighborsOnly;

	/**
	 * {@code minimal} — inventory markdown: NE id, parent chain, interface {@code nodeId}s, 1-hop neighbor equipment only;
	 * alarm JSON vertices: {@code nodeId} + {@code neType} only. {@code full} — verbose markdown + rich JSON (vendor, geo, …).
	 */
	@Value("${nova.topology.topology-output-style:minimal}")
	private String topologyOutputStyle;

	private final GraphTraversalSource g;

	private boolean compactTopologyOutput() {
		return !"full".equalsIgnoreCase(topologyOutputStyle != null ? topologyOutputStyle.trim() : "");
	}

	private final ObjectMapper objectMapper;

	/**
	 * Performs a one-time lightweight JanusGraph / Gremlin reachability check
	 * before running expensive RCA traversals.
	 */
	private final AtomicBoolean healthChecked = new AtomicBoolean(false);

	private volatile boolean janusGraphHealthy = false;

	public GraphTopologyService(GraphTraversalSource g, ObjectProvider<ObjectMapper> objectMapperProvider) {
		this.g = g;
		ObjectMapper base = objectMapperProvider.getIfAvailable();
		if (base == null) {
			log.warn("[GraphTopologyService] No ObjectMapper bean from Spring context — using standalone instance for alarm topology JSON");
			base = new ObjectMapper();
		}
		this.objectMapper = base.copy()
				.enable(SerializationFeature.INDENT_OUTPUT)
				.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.ALWAYS,
						JsonInclude.Include.ALWAYS));
	}

	/** {@code <= 0} property → effectively unlimited vertex budget for alarm JSON / closure. */
	private int capJsonSubgraphVertices() {
		return maxJsonSubgraphVerticesProp <= 0 ? Integer.MAX_VALUE : maxJsonSubgraphVerticesProp;
	}

	private int capJsonDescendantsPerSeed() {
		return maxJsonDescendantsPerSeedProp <= 0 ? Integer.MAX_VALUE : maxJsonDescendantsPerSeedProp;
	}

	private int capInventoryReportNodes() {
		return maxInventoryReportNodesProp <= 0 ? Integer.MAX_VALUE : maxInventoryReportNodesProp;
	}

	/**
	 * Tool passes {@link Integer#MAX_VALUE} for “unlimited”; positive values are used as Gremlin list caps.
	 */
	private static int effectivePerNodeCap(int maxChildrenOrNeighbors) {
		if (maxChildrenOrNeighbors <= 0 || maxChildrenOrNeighbors >= Integer.MAX_VALUE / 4) {
			return Integer.MAX_VALUE;
		}
		return Math.max(1, maxChildrenOrNeighbors);
	}

	/** Applies {@link #softCapChildrenWhenUnlimited} / {@link #softCapNeighborsWhenUnlimited} when config is "unlimited". */
	private int resolveInventoryFanoutCap(int configuredMax, int softWhenUnlimited) {
		if (configuredMax > 0 && configuredMax < Integer.MAX_VALUE / 4) {
			return configuredMax;
		}
		if (softWhenUnlimited > 0 && softWhenUnlimited < Integer.MAX_VALUE / 4) {
			return softWhenUnlimited;
		}
		return Integer.MAX_VALUE;
	}

	/** Omit Gremlin {@code .limit} when the cap is effectively unlimited (avoids Integer overflow / driver quirks). */
	private static <S, E> GraphTraversal<S, E> optionalLongLimit(GraphTraversal<S, E> t, int max) {
		if (max <= 0 || max >= Integer.MAX_VALUE / 4) {
			return t;
		}
		return t.limit((long) max);
	}

	private static String buildHierarchyHumanReadable(List<Map<String, Object>> edgesJson) {
		if (edgesJson == null || edgesJson.isEmpty()) {
			return "";
		}
		StringBuilder hb = new StringBuilder();
		for (Map<String, Object> e : edgesJson) {
			String label = Objects.toString(e.get("label"), "");
			String from = Objects.toString(e.get("from"), "");
			String to = Objects.toString(e.get("to"), "");
			if (from.isBlank() || to.isBlank()) {
				continue;
			}
			if ("PARENT_OF".equals(label)) {
				hb.append("PARENT_OF (inventory parent→child): `").append(from).append("` → `").append(to).append("`");
				if (Boolean.TRUE.equals(e.get("inferredFromNeName"))) {
					hb.append(" _(inferred from composite NE_NAME)_");
				}
				hb.append("\n");
			}
			else if ("CONNECTED_TO".equals(label)) {
				hb.append("CONNECTED_TO (OSPF link): `").append(from).append("` ↔ `").append(to).append("`\n");
			}
			else {
				hb.append(label).append(": `").append(from).append("` — `").append(to).append("`\n");
			}
		}
		return hb.toString();
	}

	private void appendConnectivityAmongNodesMarkdown(StringBuilder sb, List<String> nodeIds) {
		if (nodeIds == null || nodeIds.isEmpty()) {
			return;
		}
		List<Map<String, Object>> edges = fetchIncidentEdgesJson(new ArrayList<>(nodeIds), false);
		sb.append("\n---\n\n## Who connects to whom (edges incident to listed NEs)\n\n");
		sb.append("_Rows include **PARENT_OF** and **CONNECTED_TO** where at least one end is a listed NE; the other end may be an interface or peer not duplicated above._\n\n");
		if (edges.isEmpty()) {
			sb.append("_No **PARENT_OF** or **CONNECTED_TO** edges (and no composite `nodeId` parent prefix could be inferred). Check `NETWORK_ELEMENT.PARENT_NE_ID_FK`, `OSPF_LINK`, and Janus sync._\n\n");
			return;
		}
		sb.append("| Edge | From | To |\n| --- | --- | --- |\n");
		for (Map<String, Object> e : edges) {
			String label = Objects.toString(e.get("label"), "");
			String from = Objects.toString(e.get("from"), "");
			String to = Objects.toString(e.get("to"), "");
			sb.append("| ").append(label).append(" | `").append(from).append("` | `").append(to).append("` |\n");
		}
		sb.append("\n");
	}

	/**
	 * Expands the vertex set so incident **PARENT_OF** / **CONNECTED_TO** endpoints are more likely to be present
	 * for correlation; {@link #fetchIncidentEdgesJson} still lists edges with one endpoint outside the subgraph.
	 */
	private LinkedHashSet<String> closeSubgraphUnderIncidentEdges(LinkedHashSet<String> subgraph) {
		int cap = capJsonSubgraphVertices();
		if (subgraph == null || subgraph.isEmpty()) {
			return subgraph == null ? new LinkedHashSet<>() : subgraph;
		}
		LinkedHashSet<String> closed = new LinkedHashSet<>(subgraph);
		final int chunk = 400;
		final int maxClosureRounds = 12;
		boolean changed = true;
		int round = 0;
		while (changed && closed.size() < cap && round < maxClosureRounds) {
			round++;
			changed = false;
			List<String> batch = new ArrayList<>(closed);
			for (int i = 0; i < batch.size() && closed.size() < cap; i += chunk) {
				List<String> part = batch.subList(i, Math.min(i + chunk, batch.size()));
				Set<String> neighbors = new HashSet<>();
				try {
					g.V().has("nodeId", P.within(part)).outE("PARENT_OF").inV().values("nodeId")
							.forEachRemaining(id -> neighbors.add(Objects.toString(id, "")));
					g.V().has("nodeId", P.within(part)).inE("PARENT_OF").outV().values("nodeId")
							.forEachRemaining(id -> neighbors.add(Objects.toString(id, "")));
					g.V().has("nodeId", P.within(part)).bothE("CONNECTED_TO").otherV().values("nodeId")
							.forEachRemaining(id -> neighbors.add(Objects.toString(id, "")));
				}
				catch (Exception ex) {
					log.debug("[JanusGraph] closeSubgraph chunk failed: {}", ex.getMessage());
				}
				for (String nid : neighbors) {
					if (nid == null || nid.isBlank()) {
						continue;
					}
					nid = nid.trim();
					if (closed.size() >= cap) {
						break;
					}
					if (closed.add(nid)) {
						changed = true;
					}
				}
			}
		}
		if (round >= maxClosureRounds && changed) {
			log.warn("[JanusGraph] Subgraph edge-closure stopped after {} rounds (vertexBudget={}, currentSize={})",
					maxClosureRounds, cap, closed.size());
		}
		return closed;
	}

	/** Above this seed count, use chunked Gremlin instead of one traversal per seed (avoids 10+ minute stalls). */
	private static final int ALARM_EXPAND_GREMLIN_CHUNK = 56;

	/** Skip wide OSPF walk when intermediate vertex set is huge (each call is a full Gremlin traversal). */
	/** Skip OSPF peer expansion only on very large snapshots (prefix-discovered interfaces can push this past hundreds). */
	private static final int MAX_VERTICES_FOR_OSPF_EXPANSION = 5000;

	/**
	 * Projects standard inventory fields into plain String / Number values only.
	 * <p>
	 * JanusGraph serializes full {@code Vertex} / {@code elementMap()} responses with
	 * {@code janusgraph:RelationIdentifier} inside vertex-property metadata; the stock
	 * Gremlin driver's GraphSON stack cannot deserialize that. Using {@code project()}
	 * + {@code values()} avoids those types on the wire.
	 */
	private GraphTraversal<?, Map<String, Object>> withPlainTopologyProjection(GraphTraversal<?, Vertex> traversal) {
		return traversal
				.project("nodeId", "name", "neType", "hierarchyLevel", "vendor", "technology", "ipv4", "domain",
						"alarmCount")
				.by(__.coalesce(__.values("nodeId"), __.constant("")))
				.by(__.coalesce(__.values("name"), __.constant("")))
				.by(__.coalesce(__.values("neType"), __.constant("")))
				.by(__.coalesce(__.values("hierarchyLevel"), __.constant("")))
				.by(__.coalesce(__.values("vendor"), __.constant("")))
				.by(__.coalesce(__.values("technology"), __.constant("")))
				.by(__.coalesce(__.values("ipv4"), __.constant("")))
				.by(__.coalesce(__.values("domain"), __.constant("")))
				.by(__.coalesce(__.values("alarmCount"), __.constant(0L)));
	}

	/**
	 * Extended projection for JSON export — includes optional geo/POP fields when present on vertices.
	 */
	private GraphTraversal<?, Map<String, Object>> withJsonVertexProjection(GraphTraversal<?, Vertex> traversal) {
		return traversal
				.project("nodeId", "name", "neType", "vendor", "popName", "geographyL2Name", "domain", "technology",
						"ipv4")
				.by(__.coalesce(__.values("nodeId"), __.constant("")))
				.by(__.coalesce(__.values("name"), __.constant("")))
				.by(__.coalesce(__.values("neType"), __.constant("")))
				.by(__.coalesce(__.values("vendor"), __.constant("")))
				.by(__.coalesce(__.values("popName"), __.constant("")))
				.by(__.coalesce(__.values("geographyL2Name"), __.constant("")))
				.by(__.coalesce(__.values("domain"), __.constant("")))
				.by(__.coalesce(__.values("technology"), __.constant("")))
				.by(__.coalesce(__.values("ipv4"), __.constant("")));
	}

	private boolean vertexExistsByNodeId(String nodeId) {
		return g.V().has("nodeId", nodeId).limit(1).values("nodeId").tryNext().isPresent();
	}

	private long countOutParentOfEdges(String nodeId) {
		try {
			return g.V().has("nodeId", nodeId).out("PARENT_OF").count().tryNext().orElse(0L);
		}
		catch (Exception e) {
			log.debug("[JanusGraph] countOutParentOfEdges for {}: {}", nodeId, e.getMessage());
			return 0L;
		}
	}

	/**
	 * Interface / logical children whose {@code nodeId} starts with {@code equipmentNodeId + "_"} (inventory naming).
	 */
	private List<String> listInterfaceNodeIdsByEquipmentPrefix(String equipmentNodeId, int maxResults) {
		if (!prefixFallbackForInterfaceVertices || equipmentNodeId == null || equipmentNodeId.isBlank()) {
			return List.of();
		}
		String prefix = equipmentNodeId + "_";
		boolean unlimited = maxResults <= 0 || maxResults >= Integer.MAX_VALUE / 8;
		try {
			GraphTraversal<?, String> t = g.V().has("nodeId", TextP.startingWith(prefix)).values("nodeId");
			if (!unlimited) {
				t = t.limit((long) maxResults);
			}
			List<String> out = new ArrayList<>();
			t.forEachRemaining(id -> {
				String s = Objects.toString(id, "").trim();
				if (!s.isEmpty() && !s.equals(equipmentNodeId)) {
					out.add(s);
				}
			});
			return out;
		}
		catch (Exception e) {
			log.debug("[JanusGraph] listInterfaceNodeIdsByEquipmentPrefix prefix='{}': {}", prefix, e.getMessage());
			return List.of();
		}
	}

	private void addInterfaceVerticesByEquipmentPrefixIntoSubgraph(LinkedHashSet<String> acc, String equipmentSeed,
			int perSeedLimit, int vCap) {
		if (!prefixFallbackForInterfaceVertices || acc.size() >= vCap) {
			return;
		}
		for (String iface : listInterfaceNodeIdsByEquipmentPrefix(equipmentSeed, perSeedLimit)) {
			if (acc.size() >= vCap) {
				break;
			}
			addToSubgraph(acc, iface);
		}
	}

	private int effectivePrefixFallbackLimit(int maxChildrenScan) {
		int cap = prefixFallbackInterfacesPerEquipment > 0 ? prefixFallbackInterfacesPerEquipment : 100;
		if (maxChildrenScan > 0 && maxChildrenScan < Integer.MAX_VALUE / 8) {
			return Math.min(cap, maxChildrenScan);
		}
		return cap;
	}

	// -------------------------------------------------------------------------
	// Public API
	// -------------------------------------------------------------------------

	/**
	 * Main entry point. Accepts any number of alarming node IDs and returns a
	 * fully classified RCA result covering all scenarios.
	 */
	public RcaResult analyzeRootCause(List<String> alarmingNodeIds) {
		if (alarmingNodeIds == null || alarmingNodeIds.isEmpty()) {
			return RcaResult.empty("No alarming node IDs provided.");
		}

		ensureJanusGraphHealth();
		if (!janusGraphHealthy) {
			return RcaResult.empty("JanusGraph health check failed; verify Gremlin Server connectivity and that inventory is loaded.");
		}

		List<String> distinct = alarmingNodeIds.stream()
				.map(String::trim)
				.filter(s -> !s.isBlank())
				.distinct()
				.collect(Collectors.toList());

		// Scenario A: single node — isolated leaf, still compute blast radius
		if (distinct.size() == 1) {
			return handleIsolatedSingleNode(distinct.get(0));
		}

		// Step 1 — Build ancestor chains for every alarming node
		Map<String, List<String>> ancestorMap = buildAncestorMap(distinct);

		// Step 2 — Compute how many alarming nodes each ancestor covers
		Map<String, Long> frequency = computeAncestorFrequency(ancestorMap);

		if (frequency.isEmpty()) {
			// Scenario B: nodes have no ancestors (top-level or unconnected)
			return handleNoCommonAncestors(distinct);
		}

		// Step 3 — Identify root cause groups
		List<RcaGroup> groups = identifyRootCauses(distinct, ancestorMap, frequency);

		boolean multipleRoots = groups.size() > 1;
		String note = multipleRoots
				? String.format("%d independent fault groups identified across %d alarming nodes.", groups.size(), distinct.size())
				: String.format("Single root cause identified covering %d of %d alarming nodes.",
						groups.get(0).cascadedNodes().size() + 1, distinct.size());

		return new RcaResult(distinct, groups, multipleRoots, note);
	}

	/**
	 * Returns a quick JanusGraph inventory summary for operator checks.
	 * This is intentionally deterministic and does not involve LLM calls.
	 */
	public String inventorySummary() {
		ensureJanusGraphHealth();
		if (!janusGraphHealthy) {
			return "JanusGraph health check failed; cannot compute node/interface counts.";
		}

		try {
			long totalVertices = g.V().count().next();

			// "Node" count can vary by schema. Prefer explicit nodeId-bearing vertices.
			long nodeCountByNodeId = g.V().has("nodeId").count().next();
			long nodeCountByName = g.V().has("name").count().next();
			long effectiveNodeCount = nodeCountByNodeId > 0 ? nodeCountByNodeId : nodeCountByName;

			// Interface count heuristics (schema-dependent):
			// 1) interface-labeled vertices
			// 2) vertices carrying common interface properties
			// 3) HAS_INTERFACE edges as fallback signal
			long interfaceByLabel = g.V().hasLabel("interface").count().next();
			long interfaceByProps = g.V()
					.or(
							__.has("ifName"),
							__.has("interfaceName"),
							__.has("interfaceId"),
							__.has("ifIndex"))
					.count()
					.next();
			long interfaceByEdges = g.E().hasLabel("HAS_INTERFACE").count().next();
			long effectiveInterfaceCount = Math.max(interfaceByLabel, interfaceByProps);

			return String.format(
					"""
					JanusGraph inventory summary:
					- totalVertices: %d
					- nodeCount (effective): %d
					  - nodeCountByNodeId: %d
					  - nodeCountByName: %d
					- interfaceCount (effective): %d
					  - interfaceByLabel(interface): %d
					  - interfaceByProps(ifName/interfaceName/interfaceId/ifIndex): %d
					  - hasInterfaceEdges(HAS_INTERFACE): %d
					""",
					totalVertices,
					effectiveNodeCount,
					nodeCountByNodeId,
					nodeCountByName,
					effectiveInterfaceCount,
					interfaceByLabel,
					interfaceByProps,
					interfaceByEdges);
		}
		catch (Exception e) {
			log.warn("[JanusGraph] Failed to compute inventory summary: {}", e.getMessage());
			return "Failed to compute JanusGraph inventory summary: " + e.getMessage();
		}
	}

	/**
	 * Resolves free-form user seeds to canonical JanusGraph nodeId values.
	 * Match order (exact): nodeId -> ipv4 -> name.
	 */
	public List<String> resolveToNodeIds(List<String> seeds, int maxResolved) {
		if (seeds == null || seeds.isEmpty()) {
			return List.of();
		}
		int cap = Math.max(1, maxResolved);
		LinkedHashSet<String> resolved = new LinkedHashSet<>();
		for (String raw : seeds) {
			if (resolved.size() >= cap) break;
			String seed = raw != null ? raw.trim() : "";
			if (seed.isBlank()) continue;

			List<String> byNodeId = g.V().has("nodeId", seed).limit(8).values("nodeId").toList().stream()
					.map(Object::toString)
					.toList();
			if (!byNodeId.isEmpty()) {
				resolved.addAll(byNodeId);
				continue;
			}

			List<String> byIpv4 = g.V().has("ipv4", seed).limit(8).values("nodeId").toList().stream()
					.map(Object::toString)
					.toList();
			if (!byIpv4.isEmpty()) {
				resolved.addAll(byIpv4);
				continue;
			}

			List<String> byName = g.V().has("name", seed).limit(8).values("nodeId").toList().stream()
					.map(Object::toString)
					.toList();
			if (!byName.isEmpty()) {
				resolved.addAll(byName);
			}
		}
		return resolved.stream().limit(cap).toList();
	}

	/**
	 * Runs a minimal traversal that should succeed if Gremlin + JanusGraph are reachable.
	 * This avoids noisy per-node WARN logs when the graph DB is down.
	 */
	private void ensureJanusGraphHealth() {
		if (!healthChecked.compareAndSet(false, true)) {
			return;
		}

		long startNs = System.nanoTime();
		try {
			log.info("[JanusGraph] Health check Gremlin: g.V().limit(1).count()");
			// Cheap and avoids deserializing Vertex payloads (JanusGraph RelationIdentifier in GraphSON).
			Long sample = g.V().limit(1).count().next();

			janusGraphHealthy = true;
			long elapsedMs = Math.max(1, (System.nanoTime() - startNs) / 1_000_000);
			log.info("[JanusGraph] Health check OK (limited vertex count: {}, elapsedMs: {})", sample, elapsedMs);
		}
		catch (Exception e) {
			janusGraphHealthy = false;
			long elapsedMs = Math.max(1, (System.nanoTime() - startNs) / 1_000_000);
			// Avoid noisy stack traces when Gremlin is down/unreachable.
			log.warn("[JanusGraph] Health check FAILED (elapsedMs: {}). Error: {}", elapsedMs, e.getMessage());
		}
	}

	// -------------------------------------------------------------------------
	// Step 1 — Ancestor chain building
	// -------------------------------------------------------------------------

	/**
	 * For each alarming node, walks UP the PARENT_OF edge chain and collects
	 * all ancestor node IDs. Nodes that cannot be reached in JanusGraph are
	 * assigned an empty ancestor list (logged at WARN level).
	 */
	private Map<String, List<String>> buildAncestorMap(List<String> nodeIds) {
		Map<String, List<String>> map = new LinkedHashMap<>();
		int idx = 0;
		for (String nodeId : nodeIds) {
			try {
				// Template: g.V().has("nodeId", nodeId).repeat(__.in("PARENT_OF")).emit().values("nodeId")
				if (idx < MAX_INFO_NODE_LOGS) {
					log.info("[JanusGraph][RCA] Ancestor traversal for nodeId='{}': g.V().has(\"nodeId\",\"{}\")"
							+ ".repeat(__.in(\"PARENT_OF\")).emit().values(\"nodeId\")",
							nodeId, nodeId);
				}
				else if (log.isDebugEnabled()) {
					log.debug("[JanusGraph][RCA] Ancestor traversal for nodeId='{}'", nodeId);
				}

				List<String> ancestors = g.V().has("nodeId", nodeId)
						.repeat(__.in("PARENT_OF"))
						.emit()
						.values("nodeId")
						.toList()
						.stream()
						.map(Object::toString)
						.collect(Collectors.toList());

				if (idx < MAX_INFO_NODE_LOGS) {
					List<String> sampleAncestors = ancestors.stream()
							.limit(5)
							.collect(Collectors.toList());
					log.info("[JanusGraph][RCA] Ancestors resolved for nodeId='{}': count={}, sample={}",
							nodeId, ancestors.size(), sampleAncestors);
				}
				map.put(nodeId, ancestors);
			}
			catch (Exception e) {
				log.warn("[RCA] Could not fetch ancestors for node '{}': {}", nodeId, e.getMessage());
				map.put(nodeId, Collections.emptyList());
			}
			idx++;
		}
		return map;
	}

	// -------------------------------------------------------------------------
	// Step 2 — Frequency / coverage computation
	// -------------------------------------------------------------------------

	/**
	 * Counts how many alarming nodes each ancestor appears in.
	 * An ancestor with count = N covers N of the alarming nodes' chains.
	 */
	private Map<String, Long> computeAncestorFrequency(Map<String, List<String>> ancestorMap) {
		return ancestorMap.values().stream()
				.flatMap(List::stream)
				.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
	}

	// -------------------------------------------------------------------------
	// Step 3 — Root cause identification
	// -------------------------------------------------------------------------

	private List<RcaGroup> identifyRootCauses(
			List<String> alarmingNodes,
			Map<String, List<String>> ancestorMap,
			Map<String, Long> frequency) {

		int total = alarmingNodes.size();
		long maxFreq = frequency.values().stream().mapToLong(Long::longValue).max().orElse(0);
		double maxCoverage = (double) maxFreq / total;

		if (maxCoverage >= SINGLE_ROOT_THRESHOLD) {
			// Scenario C: one root cause (cascade or shared resource)
			String rootId = findMostSpecificAncestor(frequency, maxFreq);
			return List.of(buildGroup(rootId, alarmingNodes, ancestorMap, maxCoverage, total));
		}
		else if (maxCoverage >= PARTIAL_CASCADE_THRESHOLD) {
			// Scenario D: partial cascade — dominant root + independent remainder
			return handlePartialCascade(alarmingNodes, ancestorMap, frequency, total, maxFreq);
		}
		else {
			// Scenario E: multiple independent root causes — greedy clustering
			return clusterIntoMultipleGroups(alarmingNodes, ancestorMap, frequency, total);
		}
	}

	/**
	 * Among all ancestors that share the same (maximum) frequency, returns the
	 * <em>most specific</em> one — the one deepest in the hierarchy, measured by
	 * counting how many ancestors it itself has (more ancestors = deeper = better).
	 */
	private String findMostSpecificAncestor(Map<String, Long> frequency, long targetFreq) {
		List<String> candidates = frequency.entrySet().stream()
				.filter(e -> e.getValue() == targetFreq)
				.map(Map.Entry::getKey)
				.collect(Collectors.toList());

		if (candidates.size() == 1) {
			return candidates.get(0);
		}

		// Depth of a candidate = number of ancestors it has in the graph.
		// More ancestors → deeper in hierarchy → more specific root cause.
		return candidates.stream()
				.max(Comparator.comparingLong(id -> {
					try {
						if (log.isDebugEnabled()) {
							log.debug("[JanusGraph][RCA] Depth traversal for nodeId='{}': "
									+ "g.V().has(\"nodeId\",\"{}\").repeat(__.in(\"PARENT_OF\")).emit().count().next()",
									id, id);
						}
						return g.V().has("nodeId", id)
								.repeat(__.in("PARENT_OF"))
								.emit()
								.count()
								.next();
					}
					catch (Exception e) {
						return 0L;
					}
				}))
				.orElse(candidates.get(0));
	}

	/**
	 * Handles Scenario D: one dominant root covers 50–84 % of alarming nodes.
	 * Reports the dominant group first, then re-clusters the uncovered remainder.
	 */
	private List<RcaGroup> handlePartialCascade(
			List<String> alarmingNodes,
			Map<String, List<String>> ancestorMap,
			Map<String, Long> frequency,
			int total,
			long maxFreq) {

		List<RcaGroup> groups = new ArrayList<>();

		String dominantRootId = findMostSpecificAncestor(frequency, maxFreq);
		double coverage = (double) maxFreq / total;
		RcaGroup dominant = buildGroup(dominantRootId, alarmingNodes, ancestorMap, coverage, total);
		groups.add(dominant);

		// Nodes not covered by the dominant root → re-analyse independently
		Set<String> coveredByDominant = alarmingNodes.stream()
				.filter(n -> ancestorMap.getOrDefault(n, List.of()).contains(dominantRootId)
						|| n.equals(dominantRootId))
				.collect(Collectors.toSet());

		List<String> remainder = alarmingNodes.stream()
				.filter(n -> !coveredByDominant.contains(n))
				.collect(Collectors.toList());

		if (!remainder.isEmpty()) {
			Map<String, List<String>> remainderAncestors = remainder.stream()
					.collect(Collectors.toMap(Function.identity(),
							id -> ancestorMap.getOrDefault(id, List.of())));
			Map<String, Long> remainderFreq = computeAncestorFrequency(remainderAncestors);
			groups.addAll(remainderFreq.isEmpty()
					? remainder.stream().map(this::buildIsolatedLeafGroup).collect(Collectors.toList())
					: clusterIntoMultipleGroups(remainder, remainderAncestors, remainderFreq, total));
		}

		return groups;
	}

	/**
	 * Handles Scenario E: greedy clustering of multiple independent failures.
	 *
	 * <p>Processes ancestors sorted by descending frequency. For each candidate
	 * ancestor, collects the alarming nodes it covers that haven't been assigned
	 * yet, forms a group, and removes them from the pool. Any node with no common
	 * ancestor becomes an isolated leaf group.
	 */
	private List<RcaGroup> clusterIntoMultipleGroups(
			List<String> alarmingNodes,
			Map<String, List<String>> ancestorMap,
			Map<String, Long> frequency,
			int total) {

		List<RcaGroup> groups = new ArrayList<>();
		Set<String> assigned = new HashSet<>();

		// Sort by frequency descending so the largest clusters are processed first
		List<Map.Entry<String, Long>> sortedCandidates = frequency.entrySet().stream()
				.sorted(Map.Entry.<String, Long>comparingByValue().reversed())
				.collect(Collectors.toList());

		for (Map.Entry<String, Long> entry : sortedCandidates) {
			if (assigned.size() >= alarmingNodes.size()) break;

			String candidateRootId = entry.getKey();

			List<String> coveredNodes = alarmingNodes.stream()
					.filter(n -> !assigned.contains(n))
					.filter(n -> ancestorMap.getOrDefault(n, List.of()).contains(candidateRootId))
					.collect(Collectors.toList());

			if (coveredNodes.isEmpty()) continue;

			double coverage = (double) coveredNodes.size() / total;
			groups.add(buildGroup(candidateRootId, coveredNodes, ancestorMap, coverage, total));
			assigned.addAll(coveredNodes);
		}

		// Any remaining node with no common ancestor becomes an isolated leaf
		alarmingNodes.stream()
				.filter(n -> !assigned.contains(n))
				.forEach(n -> groups.add(buildIsolatedLeafGroup(n)));

		return groups;
	}

	// -------------------------------------------------------------------------
	// Group construction
	// -------------------------------------------------------------------------

	private RcaGroup buildGroup(
			String rootCauseId,
			List<String> alarmingNodes,
			Map<String, List<String>> ancestorMap,
			double coverage,
			int totalAlarmingNodes) {

		RcaNode rootCause = fetchNode(rootCauseId);

		// Nodes (other than root itself) that list rootCauseId in their ancestor chain
		List<RcaNode> cascaded = alarmingNodes.stream()
				.filter(id -> !id.equals(rootCauseId))
				.filter(id -> ancestorMap.getOrDefault(id, List.of()).contains(rootCauseId))
				.map(this::fetchNode)
				.collect(Collectors.toList());

		List<RcaNode> blastRadius = fetchBlastRadius(rootCauseId);

		long siblingAlarmCount = countSiblingAlarms(rootCauseId);

		log.info("[JanusGraph][RCA] Group evidence: rootCauseId='{}' name='{}' alarmCount={} hierarchyLevel='{}' cascadedCount={} blastRadiusCount={} siblingAlarmCount={} coverage={}",
				rootCause.nodeId(), rootCause.name(), rootCause.alarmCount(), rootCause.hierarchyLevel(),
				cascaded.size(), blastRadius.size(), siblingAlarmCount, coverage);

		if (log.isDebugEnabled()) {
			List<String> cascadedSample = cascaded.stream().limit(5).map(RcaNode::nodeId).collect(Collectors.toList());
			List<String> blastSample = blastRadius.stream().limit(5).map(RcaNode::nodeId).collect(Collectors.toList());
			log.debug("[JanusGraph][RCA] Cascaded sample nodeIds={}, BlastRadius sample nodeIds={}", cascadedSample, blastSample);
		}

		RcaPattern pattern = determinePattern(rootCause, cascaded, totalAlarmingNodes, siblingAlarmCount, coverage);
		double confidence = computeConfidence(coverage, rootCause, cascaded.size(), siblingAlarmCount);
		String reasoning = buildReasoning(rootCause, cascaded, blastRadius, pattern, coverage, confidence, siblingAlarmCount);

		return new RcaGroup(rootCause, cascaded, blastRadius, pattern, confidence, coverage, reasoning);
	}

	private RcaGroup buildIsolatedLeafGroup(String nodeId) {
		RcaNode node = fetchNode(nodeId);
		List<RcaNode> blastRadius = fetchBlastRadius(nodeId);
		String reasoning = String.format(
				"Isolated failure on '%s'. No common ancestor with other alarming nodes. "
				+ "Blast radius: %d downstream node(s).",
				node.name(), blastRadius.size());
		return new RcaGroup(node, List.of(), blastRadius, RcaPattern.ISOLATED_LEAF, 0.80, 1.0, reasoning);
	}

	// -------------------------------------------------------------------------
	// Pattern detection
	// -------------------------------------------------------------------------

	private RcaPattern determinePattern(
			RcaNode root,
			List<RcaNode> cascaded,
			int totalAlarming,
			long siblingAlarmCount,
			double coverage) {

		if (cascaded.isEmpty()) return RcaPattern.ISOLATED_LEAF;

		// High sibling alarm ratio → shared resource even if root has no direct alarm
		double siblingRatio = (double) siblingAlarmCount / Math.max(1, totalAlarming - 1);
		if (siblingRatio >= SHARED_RESOURCE_SIBLING_RATIO) return RcaPattern.SHARED_RESOURCE;

		// Check whether alarms span across CONNECTED_TO links (link failure heuristic):
		// If cascaded nodes are NOT direct children in the hierarchy but are reachable
		// via transport links, it suggests a link-layer failure.
		boolean likelyLinkFailure = cascaded.stream()
				.anyMatch(n -> isReachableViaLink(root.nodeId(), n.nodeId())
						&& !isDirectHierarchyChild(root.nodeId(), n.nodeId()));
		if (likelyLinkFailure) return RcaPattern.LINK_FAILURE;

		if (coverage < SINGLE_ROOT_THRESHOLD) return RcaPattern.PARTIAL_CASCADE;

		return RcaPattern.SINGLE_CASCADE;
	}

	// -------------------------------------------------------------------------
	// Confidence scoring
	// -------------------------------------------------------------------------

	/**
	 * Combines four evidence signals into a 0.0–1.0 confidence score:
	 * <ul>
	 *   <li>Coverage ratio (40 % weight)</li>
	 *   <li>Root node has a direct alarm (25 % weight)</li>
	 *   <li>Cascade size — more cascaded nodes = more evidence (up to 20 %)</li>
	 *   <li>Sibling alarm evidence (15 % weight)</li>
	 * </ul>
	 */
	private double computeConfidence(
			double coverage,
			RcaNode root,
			int cascadedCount,
			long siblingAlarmCount) {

		double score = coverage * 0.40;
		if (root.alarmCount() > 0) score += 0.25;
		score += Math.min(0.20, cascadedCount * 0.05);
		if (siblingAlarmCount > 0) score += 0.15;
		return Math.min(1.0, score);
	}

	// -------------------------------------------------------------------------
	// Reasoning narrative
	// -------------------------------------------------------------------------

	private String buildReasoning(
			RcaNode root,
			List<RcaNode> cascaded,
			List<RcaNode> blastRadius,
			RcaPattern pattern,
			double coverage,
			double confidence,
			long siblingAlarmCount) {

		StringBuilder sb = new StringBuilder();
		sb.append(String.format("Pattern: %s | Coverage: %.0f%% | Confidence: %.0f%%\n",
				pattern, coverage * 100, confidence * 100));

		sb.append(String.format(
				"Node '%s' [%s / %s / %s] is present in the ancestor chain of %.0f%% of all alarming nodes.\n",
				root.name(), root.hierarchyLevel(), root.technology(), root.vendor(), coverage * 100));

		if (root.alarmCount() > 0) {
			sb.append(String.format(
					"Root node carries %d active alarm(s) — direct fault evidence supports this hypothesis.\n",
					root.alarmCount()));
		}
		else {
			sb.append("Root node has no direct alarm — cascade is inferred from topology alone. "
					+ "Cross-check with PM KPI data to confirm.\n");
		}

		if (siblingAlarmCount > 0) {
			sb.append(String.format(
					"%d sibling node(s) under the same parent are also alarming — "
					+ "consistent with a shared resource failure.\n",
					siblingAlarmCount));
		}

		if (!cascaded.isEmpty()) {
			sb.append(String.format("%d node(s) cascade from this root: %s\n",
					cascaded.size(),
					cascaded.stream().map(RcaNode::name).collect(Collectors.joining(", "))));
		}

		if (!blastRadius.isEmpty()) {
			sb.append(String.format(
					"Blast radius: %d downstream node(s) are at risk if the root cause is not resolved.\n",
					blastRadius.size()));
		}

		return sb.toString().trim();
	}

	// -------------------------------------------------------------------------
	// Node / graph fetching helpers
	// -------------------------------------------------------------------------

	private RcaNode fetchNode(String nodeId) {
		try {
			log.info("[JanusGraph][RCA] Fetch node: g.V().has(\"nodeId\",\"{}\").project(...).next()", nodeId);
			Optional<Map<String, Object>> props = withPlainTopologyProjection(g.V().has("nodeId", nodeId)).tryNext();
			if (props.isEmpty()) {
				throw new NoSuchElementException("vertex not found");
			}
			return mapToRcaNode(props.get());
		}
		catch (NoSuchElementException e) {
			log.warn("[RCA] Node '{}' not found in JanusGraph.", nodeId);
			return RcaNode.unknown(nodeId);
		}
		catch (Exception e) {
			log.warn("[RCA] Failed to fetch node '{}': {}", nodeId, e.getMessage());
			return RcaNode.unknown(nodeId);
		}
	}

	/** Walks DOWN the PARENT_OF chain to collect all downstream nodes at risk. */
	private List<RcaNode> fetchBlastRadius(String rootCauseId) {
		try {
			log.info("[JanusGraph][RCA] Blast radius traversal for rootCauseId='{}': "
					+ "g.V().has(\"nodeId\",\"{}\").repeat(__.out(\"PARENT_OF\")).emit().project(...).toList()",
					rootCauseId, rootCauseId);
			return withPlainTopologyProjection(
					g.V().has("nodeId", rootCauseId).repeat(__.out("PARENT_OF")).emit())
					.toList()
					.stream()
					.map(this::mapToRcaNode)
					.collect(Collectors.toList());
		}
		catch (Exception e) {
			log.warn("[RCA] Could not fetch blast radius for '{}': {}", rootCauseId, e.getMessage());
			return Collections.emptyList();
		}
	}

	/**
	 * Counts sibling nodes (sharing the same parent) that currently have alarms.
	 * A high count indicates a shared resource / common parent failure.
	 */
	private long countSiblingAlarms(String nodeId) {
		try {
			log.info("[JanusGraph][RCA] Count sibling alarms for nodeId='{}': "
					+ "g.V().has(\"nodeId\",\"{}\").in(\"PARENT_OF\").out(\"PARENT_OF\").has(\"alarmCount\",P.gt(0)).count().next()",
					nodeId, nodeId);
			return g.V().has("nodeId", nodeId)
					.in("PARENT_OF")          // go to parent
					.out("PARENT_OF")         // all siblings (including self)
					.has("alarmCount", P.gt(0))
					.count()
					.next();
		}
		catch (Exception e) {
			return 0L;
		}
	}

	/**
	 * Returns true if {@code fromId} can reach {@code toId} via CONNECTED_TO
	 * edges (transport / link adjacency). Used for link failure heuristic.
	 */
	private boolean isReachableViaLink(String fromId, String toId) {
		try {
			if (log.isDebugEnabled()) {
				log.debug("[JanusGraph][RCA] ReachableViaLink traversal from '{}' to '{}': "
						+ "g.V().has(\"nodeId\",\"{}\").repeat(__.both(\"CONNECTED_TO\").simplePath()).until(__.has(\"nodeId\",\"{}\")).hasNext()",
						fromId, toId, fromId, toId);
			}
			return g.V().has("nodeId", fromId)
					.repeat(__.both("CONNECTED_TO").simplePath())
					.until(__.has("nodeId", toId))
					.limit(1)
					.values("nodeId")
					.tryNext()
					.filter(v -> toId.equals(v.toString()))
					.isPresent();
		}
		catch (Exception e) {
			return false;
		}
	}

	/**
	 * Returns true if {@code childId} is a direct child of {@code parentId}
	 * in the PARENT_OF hierarchy (one hop only).
	 */
	private boolean isDirectHierarchyChild(String parentId, String childId) {
		try {
			if (log.isDebugEnabled()) {
				log.debug("[JanusGraph][RCA] DirectHierarchyChild traversal parent='{}' child='{}': "
						+ "g.V().has(\"nodeId\",\"{}\").out(\"PARENT_OF\").has(\"nodeId\",\"{}\").hasNext()",
						parentId, childId, parentId, childId);
			}
			return g.V().has("nodeId", parentId)
					.out("PARENT_OF")
					.has("nodeId", childId)
					.limit(1)
					.values("nodeId")
					.tryNext()
					.isPresent();
		}
		catch (Exception e) {
			return false;
		}
	}

	private RcaNode mapToRcaNode(Map<String, Object> props) {
		return new RcaNode(
				str(props, "nodeId"),
				str(props, "name", str(props, "nodeId")),
				str(props, "technology", "UNKNOWN"),
				str(props, "vendor", "UNKNOWN"),
				str(props, "hierarchyLevel", "UNKNOWN"),
				longVal(props, "alarmCount"));
	}

	// -------------------------------------------------------------------------
	// Inventory topology report (hierarchy + transport links, not RCA narrative)
	// -------------------------------------------------------------------------

	/**
	 * Markdown report: for each alarming NE, show attributes, parent chain (walk {@code in("PARENT_OF")}),
	 * sample children ({@code out("PARENT_OF")}), and sample link neighbors ({@code both("CONNECTED_TO")}).
	 * Does not run clustering / confidence / blast-radius RCA logic.
	 *
	 * @param maxChildrenPerNode cap on listed child vertices per NE
	 * @param maxNeighborsPerNode cap on listed CONNECTED_TO neighbors per NE
	 */
	/**
	 * OSPF {@code CONNECTED_TO} edges are stored on <b>interface</b> vertices. From equipment, walk
	 * {@code out("PARENT_OF") → both("CONNECTED_TO") → in("PARENT_OF")}; from an interface, also
	 * {@code both("CONNECTED_TO") → in("PARENT_OF")} on the start vertex.
	 */
	private List<Map<String, Object>> adjacentEquipmentViaOspfInterfaces(String nodeId, int limit) {
		try {
			boolean unlimited = limit <= 0 || limit >= Integer.MAX_VALUE / 4;
			int lim = unlimited ? Integer.MAX_VALUE : Math.max(1, limit);
			var union = g.V().has("nodeId", nodeId)
					.union(
							__.out("PARENT_OF").both("CONNECTED_TO").dedup().in("PARENT_OF"),
							__.both("CONNECTED_TO").in("PARENT_OF"))
					.dedup();
			List<Map<String, Object>> raw = withPlainTopologyProjection(
					unlimited ? union : union.limit(Math.min(500_000L, (long) lim * 3L)))
					.toList();
			List<Map<String, Object>> out = new ArrayList<>();
			for (Map<String, Object> m : raw) {
				if (nodeId.equals(str(m, "nodeId"))) {
					continue;
				}
				out.add(m);
				if (!unlimited && out.size() >= lim) {
					break;
				}
			}
			return out;
		}
		catch (Exception e) {
			log.debug("[JanusGraph] adjacentEquipmentViaOspfInterfaces failed for nodeId={}: {}", nodeId, e.getMessage());
			return List.of();
		}
	}

	/**
	 * Peer <b>equipment</b> reachable from the listed interface children via {@code CONNECTED_TO} → peer interface →
	 * {@code in("PARENT_OF")}. Uses <b>batched</b> Gremlin ({@code P.within}) so all listed interfaces are considered in
	 * a few round-trips instead of one heavy traversal per interface.
	 */
	private List<Map<String, Object>> adjacentEquipmentAggregatedFromInterfaceChildren(String equipmentNodeId,
			List<Map<String, Object>> interfaceChildren, int limit, int maxInterfaceIdsToInclude) {
		if (interfaceChildren == null || interfaceChildren.isEmpty()) {
			return List.of();
		}
		List<String> ifaceIds = new ArrayList<>();
		for (Map<String, Object> c : interfaceChildren) {
			String ifaceId = str(c, "nodeId");
			if (ifaceId.isBlank() || "UNKNOWN".equalsIgnoreCase(ifaceId)) {
				continue;
			}
			ifaceIds.add(ifaceId);
			if (maxInterfaceIdsToInclude > 0 && ifaceIds.size() >= maxInterfaceIdsToInclude) {
				break;
			}
		}
		if (ifaceIds.isEmpty()) {
			return List.of();
		}
		return adjacentEquipmentFromInterfacesBatch(ifaceIds, equipmentNodeId, limit);
	}

	private static final int ADJ_BATCH_WITHIN = 72;

	private List<Map<String, Object>> adjacentEquipmentFromInterfacesBatch(List<String> interfaceNodeIds,
			String excludeEquipmentNodeId, int maxResults) {
		boolean unlimited = maxResults <= 0 || maxResults >= Integer.MAX_VALUE / 8;
		Map<String, Map<String, Object>> byEq = new LinkedHashMap<>();
		for (int i = 0; i < interfaceNodeIds.size() && (unlimited || byEq.size() < maxResults); i += ADJ_BATCH_WITHIN) {
			List<String> chunk = interfaceNodeIds.subList(i, Math.min(i + ADJ_BATCH_WITHIN, interfaceNodeIds.size()));
			try {
				var trav = g.V().has("nodeId", P.within(chunk))
						.both("CONNECTED_TO")
						.dedup()
						.in("PARENT_OF")
						.dedup();
				if (!unlimited) {
					trav = trav.limit(Math.min(8_000L, (long) maxResults * 25L));
				}
				List<Map<String, Object>> rows = withPlainTopologyProjection(trav).toList();
				for (Map<String, Object> m : rows) {
					String pid = str(m, "nodeId");
					if (pid.isBlank() || pid.equals(excludeEquipmentNodeId)) {
						continue;
					}
					byEq.putIfAbsent(pid, m);
					if (!unlimited && byEq.size() >= maxResults) {
						break;
					}
				}
			}
			catch (Exception e) {
				log.debug("[JanusGraph] adjacentEquipmentFromInterfacesBatch chunk={}: {}", chunk.size(), e.getMessage());
			}
		}
		return List.copyOf(byEq.values());
	}

	public String formatInventoryTopologyReport(List<String> nodeIds, int maxChildrenPerNode, int maxNeighborsPerNode) {
		ensureJanusGraphHealth();
		if (!janusGraphHealthy) {
			return "## Network topology (inventory)\n\nJanusGraph is currently unavailable.";
		}
		if (nodeIds == null || nodeIds.isEmpty()) {
			return "## Network topology (inventory)\n\nNo nodes to resolve.";
		}
		long invCap = capInventoryReportNodes();
		var nodeStream = nodeIds.stream()
				.filter(Objects::nonNull)
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.distinct();
		List<String> distinct = (invCap >= Integer.MAX_VALUE - 1)
				? nodeStream.toList()
				: nodeStream.limit(invCap).toList();
		if (fastHierarchyNeighborsOnly) {
			return formatFastHierarchyAndNeighborsReport(distinct, maxNeighborsPerNode);
		}
		int maxCh = resolveInventoryFanoutCap(maxChildrenPerNode, softCapChildrenWhenUnlimited);
		int maxNbr = resolveInventoryFanoutCap(maxNeighborsPerNode, softCapNeighborsWhenUnlimited);
		StringBuilder sb = new StringBuilder();
		if (compactTopologyOutput()) {
			sb.append("## Network hierarchy\n\n");
			sb.append("_**PARENT_OF** (parent → child), **CONNECTED_TO** (OSPF interface adjacency), **1-hop neighbor equipment** via interfaces._\n\n");
		}
		else {
			sb.append("## Network topology (inventory)\n\n");
			sb.append("""
					_**PARENT_OF** — MySQL `NETWORK_ELEMENT.PARENT_NE_ID_FK`: parent equipment → child (often **router → interface**). \
					Routers with no parent row in CMDB show an empty parent chain; **children** are **interface** vertices only if those rows exist and `PARENT_NE_ID_FK` points here._

					_**CONNECTED_TO** — from **`OSPF_LINK`**: edges are created between **interface** `NE_NAME`s, not between routers. \
					**Direct** neighbors below are only non-empty if this vertex is an interface. \
					Under each **interface** child of a router, an indented line lists **OSPF `CONNECTED_TO` peers** (other interface `nodeId`s). \
					**Adjacent equipment (via OSPF)** walks: this NE → child interfaces → `CONNECTED_TO` → peer interface → parent equipment._

					_When **PARENT_OF** is missing in Janus but interface vertices still use names `{equipment}_{interface…}`, the report can list children via **`nodeId` prefix** (`nova.topology.prefix-fallback-for-interface-vertices`) so links and OSPF neighbors still appear._

					""");
		}
		sb.append("**NEs listed:** ").append(distinct.size()).append("\n\n---\n\n");
		int total = distinct.size();
		long invT0 = System.currentTimeMillis();
		for (int i = 0; i < total; i++) {
			String id = distinct.get(i);
			if (i == 0 || (i + 1) % 25 == 0 || i + 1 == total) {
				log.info("[JanusGraph] Inventory topology report progress: {}/{} (elapsedMs={})",
						i + 1, total, System.currentTimeMillis() - invT0);
			}
			appendInventoryNodeSection(sb, id, maxCh, maxNbr);
		}
		if (!compactTopologyOutput()) {
			appendConnectivityAmongNodesMarkdown(sb, distinct);
			sb.append("""
					
					_Check **MySQL**: `NETWORK_ELEMENT` should include **INTERFACE** children with `PARENT_NE_ID_FK` set; `OSPF_LINK` should reference interface NE ids. \
					Run **JanusGraph inventory sync** with `janusgraph.inventory-sync.load-ospf-links=true`. Use `force=true` or `drop-graph-before-sync=true` if the graph was loaded before edges were fixed._
					""");
		}
		return sb.toString();
	}

	/**
	 * Edge-centric fast topology report using ACTIVE links only.
	 * Query shape:
	 * g.E().has("start", within(starts)).has("status","ACTIVE")
	 * grouped by start -> srcInterface -> [protocol, peerDevice, peerInterface, bandwidth, cktId]
	 */
	public String formatActiveLinkHierarchyReport(List<String> starts) {
		ensureJanusGraphHealth();
		if (!janusGraphHealthy) {
			return "## Active link hierarchy\n\nJanusGraph is currently unavailable.";
		}
		if (starts == null || starts.isEmpty()) {
			return "## Active link hierarchy\n\nNo start nodes provided.";
		}
		List<String> distinctStarts = starts.stream()
				.filter(Objects::nonNull)
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.distinct()
				.toList();
		if (distinctStarts.isEmpty()) {
			return "## Active link hierarchy\n\nNo start nodes provided.";
		}
		try {
			log.info("[JanusGraph][Gremlin][ACTIVE_LINK_HIERARCHY] Query:\n{}",
					buildActiveLinkHierarchyQueryForLog(distinctStarts));

			Map<Object, Object> grouped = g.E()
					.has("start", P.within(distinctStarts))
					.has("status", "ACTIVE")
					.group()
					.by(__.values("start"))
					.by(__.group()
							.by(__.values("srcInterface"))
							.by(__.project("protocol", "peerDevice", "peerInterface", "bandwidth", "cktId")
									.by(__.values("relation"))
									.by(__.values("end"))
									.by(__.values("destInterface"))
									.by(__.values("srcBandwidth"))
									.by(__.values("cktId"))
									.fold()))
					.next();

			StringBuilder sb = new StringBuilder();
			sb.append("## Active link hierarchy (edge-driven)\n\n");
			sb.append("**Requested starts:** ").append(distinctStarts.size()).append("\n\n");
			Map<Object, Object> byStart = grouped;
			if (byStart == null || byStart.isEmpty()) {
				log.info("[JanusGraph][Gremlin][ACTIVE_LINK_HIERARCHY] Response: grouped starts=0");
				sb.append("_No ACTIVE links found for requested starts._\n");
				return sb.toString();
			}

			for (String start : distinctStarts) {
				Object ifaceObj = byStart.get(start);
				sb.append("### `").append(start).append("`\n\n");
				if (!(ifaceObj instanceof Map<?, ?> ifaceMap) || ifaceMap.isEmpty()) {
					sb.append("- _(no ACTIVE links)_\n\n");
					continue;
				}
				for (Map.Entry<?, ?> ifaceEntry : ifaceMap.entrySet()) {
					String srcIf = Objects.toString(ifaceEntry.getKey(), "UNKNOWN_INTERFACE");
					sb.append("- **").append(srcIf).append("**\n");
					Object rowsObj = ifaceEntry.getValue();
					if (!(rowsObj instanceof List<?> rows) || rows.isEmpty()) {
						sb.append("  - _(no peer rows)_\n");
						continue;
					}
					for (Object rowObj : rows) {
						if (!(rowObj instanceof Map<?, ?> row)) continue;
						String protocol = Objects.toString(row.get("protocol"), "");
						String peerDevice = Objects.toString(row.get("peerDevice"), "");
						String peerInterface = Objects.toString(row.get("peerInterface"), "");
						String bandwidth = Objects.toString(row.get("bandwidth"), "");
						String cktId = Objects.toString(row.get("cktId"), "");
						sb.append("  - protocol=`").append(protocol)
								.append("`, peerDevice=`").append(peerDevice)
								.append("`, peerInterface=`").append(peerInterface)
								.append("`, bandwidth=`").append(bandwidth)
								.append("`, cktId=`").append(cktId).append("`\n");
					}
				}
				sb.append("\n");
			}
			String out = sb.toString();
			log.info("[JanusGraph][Gremlin][ACTIVE_LINK_HIERARCHY] Response: grouped starts={} markdownChars={}",
					byStart.size(), out.length());
			if (log.isDebugEnabled()) {
				log.debug("[JanusGraph][Gremlin][ACTIVE_LINK_HIERARCHY] Response markdown:\n{}", out);
			}
			else {
				int previewChars = Math.min(4000, out.length());
				log.info("[JanusGraph][Gremlin][ACTIVE_LINK_HIERARCHY] Response preview (first {} chars):\n{}",
						previewChars, out.substring(0, previewChars));
			}
			return out;
		}
		catch (Exception e) {
			log.warn("[JanusGraph] ACTIVE link hierarchy query failed: {}", e.getMessage());
			return "## Active link hierarchy\n\n[Query error: " + e.getMessage() + "]";
		}
	}

	public String formatActiveLinkHierarchyReportBatched(List<String> starts, int batchSize) {
		ensureJanusGraphHealth();
		if (!janusGraphHealthy) {
			return "## Active link hierarchy\n\nJanusGraph is currently unavailable.";
		}
		if (starts == null || starts.isEmpty()) {
			return "## Active link hierarchy\n\nNo start nodes provided.";
		}
		int size = Math.max(1, batchSize);
		List<String> distinct = starts.stream()
				.filter(Objects::nonNull)
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.distinct()
				.toList();
		if (distinct.isEmpty()) {
			return "## Active link hierarchy\n\nNo start nodes provided.";
		}

		int total = distinct.size();
		int batches = (total + size - 1) / size;
		StringBuilder sb = new StringBuilder();
		sb.append("## Active link hierarchy (edge-driven, batched)\n\n");
		sb.append("**Total starts:** ").append(total).append(" | **batchSize:** ").append(size)
				.append(" | **batches:** ").append(batches).append("\n\n");

		long t0 = System.currentTimeMillis();
		for (int b = 0; b < batches; b++) {
			int from = b * size;
			int to = Math.min(total, from + size);
			List<String> chunk = distinct.subList(from, to);
			sb.append("### Batch ").append(b + 1).append("/").append(batches)
					.append(" (seeds ").append(from + 1).append("–").append(to).append(")\n\n");
			long tb = System.currentTimeMillis();
			String part = formatActiveLinkHierarchyReport(chunk);
			long elapsed = Math.max(0L, System.currentTimeMillis() - tb);
			sb.append(part).append("\n");
			log.info("[JanusGraph][ACTIVE_LINK_HIERARCHY] Batch {}/{} complete: seeds={} elapsedMs={}",
					b + 1, batches, chunk.size(), elapsed);
		}
		log.info("[JanusGraph][ACTIVE_LINK_HIERARCHY] Batched report complete: totalSeeds={} batches={} elapsedMs={}",
				total, batches, Math.max(0L, System.currentTimeMillis() - t0));
		return sb.toString();
	}

	private static String buildActiveLinkHierarchyQueryForLog(List<String> starts) {
		String withinBlock = starts.stream()
				.map(s -> "    '" + s.replace("'", "\\'") + "'")
				.collect(Collectors.joining(",\n"));
		return """
				g.E().
				  has('start', within(
				%s
				  )).
				  has('status','ACTIVE').
				  group().
				    by(values('start')).
				    by(
				      group().
				        by(values('srcInterface')).
				        by(
				          project(
				            'protocol',
				            'peerDevice',
				            'peerInterface',
				            'bandwidth',
				            'cktId'
				          ).
				            by(values('relation')).
				            by(values('end')).
				            by(values('destInterface')).
				            by(values('srcBandwidth')).
				            by(values('cktId')).
				          fold()
				        )
				    )
				""".formatted(withinBlock);
	}

	/**
	 * Optimized inventory output for alarm-driven topology:
	 * - hierarchy chain (root -> ... -> seed)
	 * - 1-hop neighbor equipment via OSPF interfaces
	 *
	 * Skips heavy sections (full child listing, direct interface edge dumps, verbose prose).
	 */
	private String formatFastHierarchyAndNeighborsReport(List<String> nodeIds, int maxNeighborsPerNode) {
		List<String> distinct = nodeIds == null ? List.of() : nodeIds;
		int maxNbr = resolveInventoryFanoutCap(maxNeighborsPerNode, softCapNeighborsWhenUnlimited);
		StringBuilder sb = new StringBuilder();
		sb.append("## Network hierarchy + 1-hop neighbors\n\n");
		sb.append("**NEs listed:** ").append(distinct.size()).append("\n\n---\n\n");
		long t0 = System.currentTimeMillis();
		for (int i = 0; i < distinct.size(); i++) {
			String nodeId = distinct.get(i);
			if (i == 0 || (i + 1) % 25 == 0 || i + 1 == distinct.size()) {
				log.info("[JanusGraph] Fast topology report progress: {}/{} (elapsedMs={})",
						i + 1, distinct.size(), System.currentTimeMillis() - t0);
			}
			appendFastNodeSection(sb, nodeId, maxNbr);
		}
		return sb.toString();
	}

	private void appendFastNodeSection(StringBuilder sb, String nodeId, int maxNbr) {
		sb.append("### `").append(nodeId).append("`\n\n");
		try {
			// Single traversal for full ancestor chain; also doubles as existence check.
			List<String> ancestors = g.V().has("nodeId", nodeId)
					.repeat(__.in("PARENT_OF"))
					.emit()
					.values("nodeId")
					.toList()
					.stream()
					.map(Object::toString)
					.toList();
			if (ancestors.isEmpty()) {
				sb.append("- **In JanusGraph:** not found\n\n");
				return;
			}
			Collections.reverse(ancestors);
			if (ancestors.size() <= 1) {
				sb.append("- **Hierarchy:** `").append(nodeId).append("` _(root/no parent edge)_\n");
			}
			else {
				sb.append("- **Hierarchy:** ");
				sb.append(ancestors.stream()
						.filter(x -> !nodeId.equals(x))
						.map(x -> "`" + x + "`")
						.collect(Collectors.joining(" -> ")));
				sb.append(" -> `").append(nodeId).append("`\n");
			}

			List<Map<String, Object>> neighbors = adjacentEquipmentViaOspfInterfaces(nodeId, maxNbr);
			LinkedHashSet<String> eqIds = new LinkedHashSet<>();
			for (Map<String, Object> row : neighbors) {
				String id = str(row, "nodeId");
				if (!id.isBlank() && !nodeId.equals(id)) {
					eqIds.add(id);
				}
			}
			sb.append("- **1-hop neighbor equipment:** ");
			if (eqIds.isEmpty()) {
				sb.append("_(none)_\n\n");
			}
			else {
				sb.append(eqIds.stream().map(x -> "`" + x + "`").collect(Collectors.joining(", "))).append("\n\n");
			}
		}
		catch (Exception e) {
			log.warn("[JanusGraph] Fast node section failed for nodeId={}: {}", nodeId, e.getMessage());
			sb.append("- **Error:** ").append(e.getMessage()).append("\n\n");
		}
	}

	/**
	 * JSON subgraph for alarming NEs: expands distinct {@code nodeId}s from alarms with
	 * {@code PARENT_OF} ancestors/descendants, child interfaces, and OSPF peers (via {@code CONNECTED_TO}),
	 * then emits vertices and incident edges (endpoints may extend beyond {@code vertices}). Vertex map keys are inventory {@code nodeId} strings
	 * ({@code NE_NAME}). {@code CONNECTED_TO} edges use {@code relation: "OSPF"} (loaded from
	 * {@code OSPF_LINK}). Optional {@code popName} / {@code geographyL2Name} appear when stored on vertices.
	 */
	public String formatAlarmTopologySubgraphJson(List<String> seedNodeIds, int maxChildrenPerNode,
			int maxNeighborsPerNode) {
		ensureJanusGraphHealth();
		Map<String, Object> shell = new LinkedHashMap<>();
		shell.put("vertices", Map.of());
		shell.put("edges", List.of());
		if (!janusGraphHealthy) {
			shell.put("note", "JanusGraph unavailable.");
			String out = writeJsonSafe(shell);
			log.info("[JanusGraph→LLM] Alarm subgraph JSON (unhealthy) chars={} body={}", out.length(), out);
			return out;
		}
		if (seedNodeIds == null || seedNodeIds.isEmpty()) {
			shell.put("note", "No seed node IDs.");
			String out = writeJsonSafe(shell);
			log.info("[JanusGraph→LLM] Alarm subgraph JSON (no seeds) chars={} body={}", out.length(), out);
			return out;
		}
		long invCap = capInventoryReportNodes();
		var seedStream = seedNodeIds.stream()
				.filter(Objects::nonNull)
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.distinct();
		List<String> seeds = (invCap >= Integer.MAX_VALUE - 1)
				? seedStream.toList()
				: seedStream.limit(invCap).toList();
		int maxNbr = resolveInventoryFanoutCap(maxNeighborsPerNode, softCapNeighborsWhenUnlimited);
		int maxCh = resolveInventoryFanoutCap(maxChildrenPerNode, softCapChildrenWhenUnlimited);
		try {
			LinkedHashSet<String> subgraph = expandAlarmSubgraphVertices(seeds, maxCh, maxNbr);
			subgraph = closeSubgraphUnderIncidentEdges(subgraph);
			if (subgraph.isEmpty()) {
				shell.put("note", "No JanusGraph vertices for alarm seeds.");
				String out = writeJsonSafe(shell);
				log.info("[JanusGraph→LLM] Alarm subgraph JSON (empty subgraph) chars={} body={}", out.length(), out);
				return out;
			}
			List<String> idList = new ArrayList<>(subgraph);
			Map<String, Map<String, Object>> verticesJson = fetchVerticesForJsonBatch(idList);
			List<Map<String, Object>> edgesJson = fetchIncidentEdgesJson(idList, true);
			var root = new LinkedHashMap<String, Object>();
			root.put("vertices", verticesJson);
			root.put("edges", edgesJson);
			root.put("hierarchyHumanReadable", buildHierarchyHumanReadable(edgesJson));
			root.put("vertexKey", "nodeId");
			root.put("seedNodeIds", seeds);
			String json = writeJsonSafe(root);
			log.info("[JanusGraph→LLM] Alarm subgraph JSON — subgraphNodeCount={}, vertexProps={}, edgeCount={}, "
							+ "jsonChars={}, seedCount={}",
					subgraph.size(), verticesJson.size(), edgesJson.size(), json.length(), seeds.size());
			if (!json.isEmpty()) {
				if (log.isDebugEnabled()) {
					log.debug("[JanusGraph→LLM] Alarm subgraph JSON (full body):\n{}", json);
				}
				else if (json.length() <= 8_000) {
					log.info("[JanusGraph→LLM] Alarm subgraph JSON (full body):\n{}", json);
				}
				else {
					log.info("[JanusGraph→LLM] Alarm subgraph JSON (omitted at INFO; {} chars). Preview:\n{}…",
							json.length(), json.substring(0, Math.min(3_500, json.length())));
				}
			}
			return json;
		}
		catch (Exception e) {
			log.warn("[JanusGraph] Alarm topology JSON failed: {}", e.getMessage());
			shell.put("error", e.getMessage());
			String out = writeJsonSafe(shell);
			log.info("[JanusGraph→LLM] Alarm subgraph JSON (error) chars={} body={}", out.length(), out);
			return out;
		}
	}

	private String writeJsonSafe(Object o) {
		try {
			return objectMapper.writeValueAsString(o);
		}
		catch (JsonProcessingException e) {
			return "{\"vertices\":{},\"edges\":[],\"error\":\"json serialization failed\"}";
		}
	}

	private LinkedHashSet<String> expandAlarmSubgraphVertices(List<String> seeds, int maxChildrenScan,
			int maxOspfNeighborsPerNode) {
		if (seeds.size() >= ALARM_EXPAND_GREMLIN_CHUNK) {
			log.info("[JanusGraph] Large alarm seed set ({}); using chunked Gremlin expansion", seeds.size());
			return expandAlarmSubgraphVerticesChunked(seeds, maxChildrenScan, maxOspfNeighborsPerNode);
		}
		int vCap = capJsonSubgraphVertices();
		int dCap = capJsonDescendantsPerSeed();
		LinkedHashSet<String> acc = new LinkedHashSet<>();
		for (String seed : seeds) {
			if (acc.size() >= vCap) {
				break;
			}
			if (!vertexExistsByNodeId(seed)) {
				continue;
			}
			addToSubgraph(acc, seed);
			try {
				g.V().has("nodeId", seed).repeat(__.in("PARENT_OF")).emit().values("nodeId")
						.forEachRemaining(id -> addToSubgraph(acc, Objects.toString(id, "")));
			}
			catch (Exception e) {
				log.debug("[JanusGraph] ancestor expand for {}: {}", seed, e.getMessage());
			}
			if (acc.size() >= vCap) {
				break;
			}
			try {
				var desc = g.V().has("nodeId", seed).repeat(__.out("PARENT_OF")).emit().values("nodeId");
				if (dCap < Integer.MAX_VALUE / 4) {
					desc = desc.limit((long) dCap);
				}
				desc.forEachRemaining(id -> addToSubgraph(acc, Objects.toString(id, "")));
			}
			catch (Exception e) {
				log.debug("[JanusGraph] descendant expand for {}: {}", seed, e.getMessage());
			}
			if (prefixFallbackForInterfaceVertices && countOutParentOfEdges(seed) == 0) {
				addInterfaceVerticesByEquipmentPrefixIntoSubgraph(acc, seed,
						effectivePrefixFallbackLimit(maxChildrenScan), vCap);
			}
		}
		List<String> snapshot = new ArrayList<>(acc);
		for (String n : snapshot) {
			if (acc.size() >= vCap) {
				break;
			}
			try {
				optionalLongLimit(g.V().has("nodeId", n).out("PARENT_OF").values("nodeId"), maxChildrenScan)
						.forEachRemaining(id -> addToSubgraph(acc, Objects.toString(id, "")));
			}
			catch (Exception ignored) {
			}
		}
		snapshot = new ArrayList<>(acc);
		expandOspfAdjacencyForSnapshot(acc, snapshot, seeds, maxOspfNeighborsPerNode, vCap);
		return acc;
	}

	private LinkedHashSet<String> expandAlarmSubgraphVerticesChunked(List<String> seeds, int maxChildrenScan,
			int maxOspfNeighborsPerNode) {
		int vCap = capJsonSubgraphVertices();
		int dCap = capJsonDescendantsPerSeed();
		LinkedHashSet<String> acc = new LinkedHashSet<>();
		for (int i = 0; i < seeds.size() && acc.size() < vCap; i += ALARM_EXPAND_GREMLIN_CHUNK) {
			List<String> part = seeds.subList(i, Math.min(i + ALARM_EXPAND_GREMLIN_CHUNK, seeds.size()));
			try {
				g.V().has("nodeId", P.within(part)).values("nodeId")
						.forEachRemaining(id -> addToSubgraph(acc, Objects.toString(id, "")));
			}
			catch (Exception e) {
				log.debug("[JanusGraph] chunked seed presence: {}", e.getMessage());
			}
			if (acc.size() >= vCap) {
				break;
			}
			try {
				var anc = g.V().has("nodeId", P.within(part)).repeat(__.in("PARENT_OF")).emit().values("nodeId");
				anc.forEachRemaining(id -> addToSubgraph(acc, Objects.toString(id, "")));
			}
			catch (Exception e) {
				log.debug("[JanusGraph] chunked ancestor expand: {}", e.getMessage());
			}
			if (acc.size() >= vCap) {
				break;
			}
			try {
				var desc = g.V().has("nodeId", P.within(part)).repeat(__.out("PARENT_OF")).emit().values("nodeId");
				if (dCap < Integer.MAX_VALUE / 4) {
					desc = desc.limit((long) dCap * part.size());
				}
				desc.forEachRemaining(id -> addToSubgraph(acc, Objects.toString(id, "")));
			}
			catch (Exception e) {
				log.debug("[JanusGraph] chunked descendant expand: {}", e.getMessage());
			}
		}
		if (prefixFallbackForInterfaceVertices) {
			int lim = effectivePrefixFallbackLimit(maxChildrenScan);
			for (String seed : seeds) {
				if (acc.size() >= vCap) {
					break;
				}
				if (!vertexExistsByNodeId(seed)) {
					continue;
				}
				if (countOutParentOfEdges(seed) > 0) {
					continue;
				}
				addInterfaceVerticesByEquipmentPrefixIntoSubgraph(acc, seed, lim, vCap);
			}
		}
		List<String> snapshot = new ArrayList<>(acc);
		for (int i = 0; i < snapshot.size() && acc.size() < vCap; i += ALARM_EXPAND_GREMLIN_CHUNK) {
			List<String> part = snapshot.subList(i, Math.min(i + ALARM_EXPAND_GREMLIN_CHUNK, snapshot.size()));
			try {
				var childTrav = g.V().has("nodeId", P.within(part)).out("PARENT_OF").values("nodeId").dedup();
				if (maxChildrenScan > 0 && maxChildrenScan < Integer.MAX_VALUE / 8) {
					childTrav = childTrav.limit((long) part.size() * (long) maxChildrenScan);
				}
				childTrav.forEachRemaining(id -> addToSubgraph(acc, Objects.toString(id, "")));
			}
			catch (Exception ex) {
				log.debug("[JanusGraph] chunked child expand: {}", ex.getMessage());
			}
		}
		snapshot = new ArrayList<>(acc);
		expandOspfAdjacencyForSnapshot(acc, snapshot, seeds, maxOspfNeighborsPerNode, vCap);
		return acc;
	}

	private void expandOspfAdjacencyForSnapshot(LinkedHashSet<String> acc, List<String> snapshot, List<String> originalSeeds,
			int maxOspfNeighborsPerNode, int vCap) {
		if (snapshot.size() > MAX_VERTICES_FOR_OSPF_EXPANSION) {
			log.info("[JanusGraph] Skipping wide OSPF adjacency expansion (vertices={}; cap={})",
					snapshot.size(), MAX_VERTICES_FOR_OSPF_EXPANSION);
			return;
		}
		Set<String> seedSet = new HashSet<>(originalSeeds);
		// When >400 we used to only walk original seeds (routers), but OSPF lives on interface vertices — walk all
		// vertices until the snapshot is very large so prefix-discovered interfaces still pull peer equipment.
		boolean onlySeeds = snapshot.size() > 2500;
		for (String n : snapshot) {
			if (acc.size() >= vCap) {
				break;
			}
			if (onlySeeds && !seedSet.contains(n)) {
				continue;
			}
			for (Map<String, Object> row : adjacentEquipmentViaOspfInterfaces(n, maxOspfNeighborsPerNode)) {
				if (acc.size() >= vCap) {
					break;
				}
				Object oid = row.get("nodeId");
				if (oid != null) {
					addToSubgraph(acc, oid.toString());
				}
			}
		}
	}

	private void addToSubgraph(LinkedHashSet<String> acc, String nodeId) {
		if (nodeId == null || nodeId.isBlank()) {
			return;
		}
		int vCap = capJsonSubgraphVertices();
		if (acc.size() >= vCap) {
			return;
		}
		acc.add(nodeId.trim());
	}

	private Map<String, Map<String, Object>> fetchVerticesForJsonBatch(List<String> idList) {
		Map<String, Map<String, Object>> out = new LinkedHashMap<>();
		if (idList.isEmpty()) {
			return out;
		}
		try {
			List<Map<String, Object>> rows = withJsonVertexProjection(g.V().has("nodeId", P.within(idList))).toList();
			for (Map<String, Object> m : rows) {
				String nid = str(m, "nodeId");
				if (nid.isBlank()) {
					continue;
				}
				Map<String, Object> body = new LinkedHashMap<>();
				body.put("relation", "VERTEX");
				if (compactTopologyOutput()) {
					body.put("neType", str(m, "neType", ""));
					out.put(nid, body);
					continue;
				}
				body.put("neName", str(m, "name", nid));
				body.put("neType", str(m, "neType", ""));
				body.put("vendor", str(m, "vendor", ""));
				String pop = str(m, "popName", "");
				if (pop.isBlank() || "UNKNOWN".equalsIgnoreCase(pop)) {
					pop = "";
				}
				body.put("popName", pop);
				String geo = str(m, "geographyL2Name", "");
				if (geo.isBlank() || "UNKNOWN".equalsIgnoreCase(geo)) {
					geo = str(m, "domain", "");
				}
				if ("UNKNOWN".equalsIgnoreCase(geo)) {
					geo = "";
				}
				body.put("geographyL2Name", geo);
				body.put("technology", str(m, "technology", ""));
				body.put("ipv4", str(m, "ipv4", ""));
				out.put(nid, body);
			}
		}
		catch (Exception e) {
			log.warn("[JanusGraph] Batch vertex projection for JSON failed: {}", e.getMessage());
		}
		return out;
	}

	/**
	 * All {@code PARENT_OF} / {@code CONNECTED_TO} edges that touch any vertex in {@code idList}.
	 * The other endpoint may be absent from {@code vertices} in the JSON — that is intentional so router-only
	 * subgraphs still show links to interfaces and OSPF peers.
	 *
	 * @param logDisconnectedSubgraphDiagnostics when {@code true}, emit WARN when the subgraph has no real edges
	 *        (inference / empty-graph hints). Inventory markdown calls this with {@code false} so JSON formatting
	 *        does not duplicate the same warning.
	 */
	private List<Map<String, Object>> fetchIncidentEdgesJson(List<String> idList, boolean logDisconnectedSubgraphDiagnostics) {
		if (idList.isEmpty()) {
			return List.of();
		}
		List<Map<String, Object>> edges = new ArrayList<>();
		Set<String> dedupe = new HashSet<>();
		long[] edgeId = { 1L };
		try {
			List<Map<String, Object>> parentOut = g.V().has("nodeId", P.within(idList))
					.outE("PARENT_OF")
					.project("from", "to")
					.by(__.outV().values("nodeId"))
					.by(__.inV().values("nodeId"))
					.toList();
			List<Map<String, Object>> parentIn = g.V().has("nodeId", P.within(idList))
					.inE("PARENT_OF")
					.project("from", "to")
					.by(__.outV().values("nodeId"))
					.by(__.inV().values("nodeId"))
					.toList();
			for (Map<String, Object> row : parentOut) {
				addJsonEdge(edges, dedupe, edgeId, row, "PARENT_OF", null);
			}
			for (Map<String, Object> row : parentIn) {
				addJsonEdge(edges, dedupe, edgeId, row, "PARENT_OF", null);
			}
		}
		catch (Exception e) {
			log.warn("[JanusGraph] PARENT_OF edge query failed for subgraph (vertices={}): {}", idList.size(), e.toString());
		}
		try {
			List<Map<String, Object>> linkEdges = g.V().has("nodeId", P.within(idList))
					.bothE("CONNECTED_TO")
					.dedup()
					.project("from", "to")
					.by(__.outV().values("nodeId"))
					.by(__.inV().values("nodeId"))
					.toList();
			for (Map<String, Object> row : linkEdges) {
				addJsonEdge(edges, dedupe, edgeId, row, "CONNECTED_TO", "OSPF");
			}
		}
		catch (Exception e) {
			log.warn("[JanusGraph] CONNECTED_TO edge query failed for subgraph (vertices={}): {}", idList.size(), e.toString());
		}
		if (edges.isEmpty() && idList.size() > 1) {
			int inferred = appendInferredParentOfFromCompositeNodeIds(idList, edges, dedupe, edgeId);
			if (logDisconnectedSubgraphDiagnostics) {
				if (inferred > 0) {
					log.warn("[JanusGraph] Graph had no incident PARENT_OF/CONNECTED_TO for this subgraph — added {} "
							+ "synthetic **PARENT_OF** edges from composite `nodeId` prefixes (e.g. `Router_if_123` → `Router`). "
							+ "For authoritative links, fix Janus sync / MySQL `PARENT_NE_ID_FK` and `OSPF_LINK`, then reload the graph.",
							inferred);
				}
				else {
					log.warn("[JanusGraph] Subgraph has {} vertices but **zero** PARENT_OF/CONNECTED_TO edges in JanusGraph and "
							+ "no inferable composite parent prefixes — topology is disconnected in the graph.",
							idList.size());
				}
			}
			else if (log.isDebugEnabled()) {
				log.debug("[JanusGraph] Subgraph edge diagnostics suppressed for this call (inferred={}, vertices={})",
						inferred, idList.size());
			}
		}
		return edges;
	}

	/**
	 * When inventory uses composite {@code NE_NAME} / {@code nodeId} like {@code PE-Router_ge-0/0/0_510} but JanusGraph
	 * has no {@code PARENT_OF} edge, infer parent as the longest other {@code nodeId} in the set such that
	 * {@code child.startsWith(parent + "_")}.
	 */
	private static int appendInferredParentOfFromCompositeNodeIds(List<String> idList, List<Map<String, Object>> edges,
			Set<String> dedupe, long[] edgeId) {
		int added = 0;
		for (String v : idList) {
			if (v == null || v.isBlank()) {
				continue;
			}
			String bestParent = null;
			int bestLen = -1;
			for (String p : idList) {
				if (p == null || p.isBlank() || p.equals(v)) {
					continue;
				}
				if (v.length() > p.length() + 1 && v.startsWith(p + "_") && p.length() > bestLen) {
					bestParent = p;
					bestLen = p.length();
				}
			}
			if (bestParent == null) {
				continue;
			}
			String key = "PARENT_OF|" + bestParent + "|" + v;
			if (!dedupe.add(key)) {
				continue;
			}
			Map<String, Object> e = new LinkedHashMap<>();
			e.put("id", edgeId[0]++);
			e.put("from", bestParent);
			e.put("to", v);
			e.put("label", "PARENT_OF");
			e.put("relation", null);
			e.put("inferredFromNeName", Boolean.TRUE);
			e.put("properties", Map.of());
			edges.add(e);
			added++;
		}
		return added;
	}

	private static void addJsonEdge(List<Map<String, Object>> edges, Set<String> dedupe, long[] edgeId,
			Map<String, Object> row, String label, String relation) {
		String from = Objects.toString(row.get("from"), "");
		String to = Objects.toString(row.get("to"), "");
		String key = label + "|" + from + "|" + to;
		if (from.isBlank() || to.isBlank() || !dedupe.add(key)) {
			return;
		}
		Map<String, Object> e = new LinkedHashMap<>();
		e.put("id", edgeId[0]++);
		e.put("from", from);
		e.put("to", to);
		e.put("label", label);
		e.put("relation", relation);
		e.put("properties", Map.of());
		edges.add(e);
	}

	/**
	 * Hierarchy-only block: {@code nodeId}, parent chain, child interface {@code nodeId}s,
	 * 1-hop peer interfaces ({@code CONNECTED_TO}), 1-hop neighbor equipment (OSPF walk). No vendor/domain/IPv4 prose.
	 */
	private void appendInventoryNodeSectionCompact(StringBuilder sb, String nodeId, int maxCh, int maxNbr) {
		sb.append("### `").append(nodeId).append("`\n\n");
		try {
			if (!vertexExistsByNodeId(nodeId)) {
				sb.append("- **NE:** not in graph\n\n");
				return;
			}
			Map<String, Object> self = withPlainTopologyProjection(g.V().has("nodeId", nodeId)).next();
			sb.append("- **NE:** `").append(nodeId).append("`");
			String neTypeLine = str(self, "neType", "");
			if (!neTypeLine.isBlank()) {
				sb.append(" · ").append(neTypeLine);
			}
			sb.append("\n");

			List<String> parentsUp = new ArrayList<>();
			String cursor = nodeId;
			for (int d = 0; d < 64; d++) {
				Optional<Object> parentOpt = g.V().has("nodeId", cursor).in("PARENT_OF").values("nodeId").tryNext();
				if (parentOpt.isEmpty()) {
					break;
				}
				String p = parentOpt.get().toString();
				parentsUp.add(p);
				cursor = p;
			}
			Collections.reverse(parentsUp);
			if (parentsUp.isEmpty()) {
				sb.append("- **Hierarchy ↑:** _(none — root or missing PARENT_OF)_\n");
			}
			else {
				sb.append("- **Hierarchy ↑:** ");
				sb.append(parentsUp.stream().map(x -> "`" + x + "`").collect(Collectors.joining(" → ")));
				sb.append(" → `").append(nodeId).append("`\n");
			}

			String neTypeSelf = str(self, "neType", "").toUpperCase(Locale.ROOT);
			boolean equipmentLike = neTypeSelf.contains("ROUTER") || neTypeSelf.contains("SWITCH");

			List<Map<String, Object>> children = withPlainTopologyProjection(
					optionalLongLimit(g.V().has("nodeId", nodeId).out("PARENT_OF"), maxCh))
					.toList();
			boolean listedViaNodeIdPrefix = false;
			if (children.isEmpty() && equipmentLike && prefixFallbackForInterfaceVertices) {
				int prefixLim = maxCh > 0 && maxCh < Integer.MAX_VALUE / 8 ? maxCh : effectivePrefixFallbackLimit(maxCh);
				List<String> ifaceIds = listInterfaceNodeIdsByEquipmentPrefix(nodeId, prefixLim);
				if (!ifaceIds.isEmpty()) {
					listedViaNodeIdPrefix = true;
					children = withPlainTopologyProjection(g.V().has("nodeId", P.within(ifaceIds))).toList();
				}
			}

			sb.append("- **Interfaces** (").append(children.size());
			if (listedViaNodeIdPrefix) {
				sb.append(" · via `nodeId` prefix");
			}
			sb.append("):\n");
			if (children.isEmpty()) {
				sb.append("  - _(none)_\n");
			}
			else {
				for (Map<String, Object> c : children) {
					sb.append("  - `").append(str(c, "nodeId")).append("`\n");
				}
			}

			List<Map<String, Object>> peersDirect = withPlainTopologyProjection(
					optionalLongLimit(g.V().has("nodeId", nodeId).both("CONNECTED_TO").dedup(), maxNbr))
					.toList();
			LinkedHashSet<String> peerIfaceIds = new LinkedHashSet<>();
			for (Map<String, Object> row : peersDirect) {
				String nid = str(row, "nodeId");
				if (!nodeId.equals(nid)) {
					peerIfaceIds.add(nid);
				}
			}
			sb.append("- **Peer interfaces (CONNECTED_TO, 1-hop):** ");
			if (peerIfaceIds.isEmpty()) {
				sb.append("_(none)_\n");
			}
			else {
				sb.append(peerIfaceIds.stream().map(x -> "`" + x + "`").collect(Collectors.joining(", ")));
				sb.append("\n");
			}

			List<Map<String, Object>> nbrsViaOspf = adjacentEquipmentViaOspfInterfaces(nodeId, maxNbr);
			if (nbrsViaOspf.isEmpty() && equipmentLike && !children.isEmpty()) {
				nbrsViaOspf = adjacentEquipmentAggregatedFromInterfaceChildren(nodeId, children, maxNbr,
						inventoryMaxInterfacesForNeighborWalk);
			}
			sb.append("- **Neighbor equipment (1-hop via OSPF):** ");
			if (nbrsViaOspf.isEmpty()) {
				sb.append("_(none)_\n");
			}
			else {
				LinkedHashSet<String> eqIds = new LinkedHashSet<>();
				for (Map<String, Object> n : nbrsViaOspf) {
					String eid = str(n, "nodeId");
					if (!eid.isBlank()) {
						eqIds.add(eid);
					}
				}
				sb.append(eqIds.stream().map(x -> "`" + x + "`").collect(Collectors.joining(", ")));
				sb.append("\n");
			}
			sb.append("\n");
		}
		catch (Exception e) {
			log.warn("[JanusGraph] Failed building compact inventory section for nodeId={}", nodeId, e);
			sb.append("- **Error:** ").append(e.getMessage()).append("\n\n");
		}
	}

	private void appendInventoryNodeSection(StringBuilder sb, String nodeId, int maxCh, int maxNbr) {
		if (compactTopologyOutput()) {
			appendInventoryNodeSectionCompact(sb, nodeId, maxCh, maxNbr);
			return;
		}
		sb.append("### `").append(nodeId).append("`\n\n");
		try {
			if (!vertexExistsByNodeId(nodeId)) {
				sb.append("- **In JanusGraph:** not found\n\n");
				return;
			}
			Map<String, Object> self = withPlainTopologyProjection(g.V().has("nodeId", nodeId)).next();
			sb.append("- **This NE:** ").append(summarizeElementMap(self)).append("\n");

			List<String> parentsUp = new ArrayList<>();
			String cursor = nodeId;
			for (int d = 0; d < 64; d++) {
				Optional<Object> parentOpt = g.V().has("nodeId", cursor).in("PARENT_OF").values("nodeId").tryNext();
				if (parentOpt.isEmpty()) {
					break;
				}
				String p = parentOpt.get().toString();
				parentsUp.add(p);
				cursor = p;
			}
			Collections.reverse(parentsUp);
			if (parentsUp.isEmpty()) {
				sb.append("- **Parent chain (PARENT_OF ↑):** _(no parent — top of hierarchy or missing edges)_\n");
			}
			else {
				sb.append("- **Parent chain (root → … → this NE):** ");
				sb.append(parentsUp.stream().map(x -> "`" + x + "`").collect(Collectors.joining(" → ")));
				sb.append(" → `").append(nodeId).append("`\n");
			}

			String neTypeSelf = str(self, "neType", "").toUpperCase(Locale.ROOT);
			boolean equipmentLike = neTypeSelf.contains("ROUTER") || neTypeSelf.contains("SWITCH");

			List<Map<String, Object>> children = withPlainTopologyProjection(
					optionalLongLimit(g.V().has("nodeId", nodeId).out("PARENT_OF"), maxCh))
					.toList();
			long childTotal = children.size();
			if (maxCh < Integer.MAX_VALUE / 8 && !children.isEmpty() && children.size() >= maxCh) {
				try {
					childTotal = g.V().has("nodeId", nodeId).out("PARENT_OF").count().tryNext()
							.orElse((long) children.size());
				}
				catch (Exception e) {
					log.debug("[JanusGraph] child count for {}: {}", nodeId, e.getMessage());
					childTotal = children.size();
				}
			}
			boolean listedViaNodeIdPrefix = false;
			int prefixLim = 0;
			if (children.isEmpty() && equipmentLike && prefixFallbackForInterfaceVertices) {
				prefixLim = maxCh > 0 && maxCh < Integer.MAX_VALUE / 8 ? maxCh : effectivePrefixFallbackLimit(maxCh);
				List<String> ifaceIds = listInterfaceNodeIdsByEquipmentPrefix(nodeId, prefixLim);
				if (!ifaceIds.isEmpty()) {
					listedViaNodeIdPrefix = true;
					childTotal = ifaceIds.size();
					children = withPlainTopologyProjection(g.V().has("nodeId", P.within(ifaceIds))).toList();
				}
			}

			sb.append("- **").append(equipmentLike ? "Interfaces / children" : "Children");
			sb.append("** (").append(listedViaNodeIdPrefix
					? "`nodeId` prefix `" + nodeId + "_*` (no **PARENT_OF** from this NE in graph — fix sync for authoritative hierarchy)"
					: "`out PARENT_OF`");
			sb.append("), showing ").append(children.size());
			if (!listedViaNodeIdPrefix && childTotal > children.size()) {
				sb.append(" of ").append(childTotal);
			}
			else if (listedViaNodeIdPrefix && prefixLim > 0 && childTotal >= prefixLim) {
				sb.append(" _(prefix cap ").append(prefixLim).append(" — more may exist)_");
			}
			sb.append(":**\n");
			if (children.isEmpty()) {
				sb.append("  - _(none — no `PARENT_OF` children and no `").append(nodeId)
						.append("_*` interface vertices in Janus; add **INTERFACE** rows + `PARENT_NE_ID_FK` in MySQL, then re-sync)_\n");
			}
			else {
				int ospfLineBudget = inventoryMaxInterfacesForOspfPeerLines <= 0 ? Integer.MAX_VALUE
						: inventoryMaxInterfacesForOspfPeerLines;
				int ospfLines = 0;
				for (Map<String, Object> c : children) {
					sb.append("  - ").append(summarizeElementMap(c)).append("\n");
					String childId = str(c, "nodeId");
					if (ospfLines < ospfLineBudget && !childId.isBlank() && !"UNKNOWN".equalsIgnoreCase(childId)) {
						appendOspfPeersIndented(sb, childId, maxNbr);
						ospfLines++;
					}
				}
				if (ospfLineBudget < Integer.MAX_VALUE && children.size() > ospfLineBudget) {
					sb.append("  - _(OSPF peer lines omitted for remaining interfaces; raise ")
							.append("`nova.topology.inventory-max-interfaces-for-ospf-peer-lines` or set 0 for all)_\n");
				}
			}

			List<Map<String, Object>> nbrsDirect = withPlainTopologyProjection(
					optionalLongLimit(g.V().has("nodeId", nodeId).both("CONNECTED_TO").dedup(), maxNbr))
					.toList();
			sb.append("- **Direct CONNECTED_TO** (only if this NE is an **interface**), showing ").append(nbrsDirect.size()).append(":**\n");
			if (nbrsDirect.isEmpty()) {
				sb.append("  - _(none — expected for **ROUTER/SWITCH**; OSPF edges are on interface vertices)_\n");
			}
			else {
				for (Map<String, Object> n : nbrsDirect) {
					String nid = str(n, "nodeId");
					if (nodeId.equals(nid)) {
						continue;
					}
					sb.append("  - ").append(summarizeElementMap(n)).append("\n");
				}
			}

			List<Map<String, Object>> nbrsViaOspf = adjacentEquipmentViaOspfInterfaces(nodeId, maxNbr);
			if (nbrsViaOspf.isEmpty() && equipmentLike && !children.isEmpty()) {
				nbrsViaOspf = adjacentEquipmentAggregatedFromInterfaceChildren(nodeId, children, maxNbr,
						inventoryMaxInterfacesForNeighborWalk);
			}
			sb.append("- **Adjacent equipment (OSPF via interfaces), showing ").append(nbrsViaOspf.size()).append(":**\n");
			if (nbrsViaOspf.isEmpty()) {
				sb.append("  - _(none — need interface vertices + `CONNECTED_TO` from OSPF sync, or this NE has no OSPF-linked paths)_\n");
			}
			else {
				for (Map<String, Object> n : nbrsViaOspf) {
					sb.append("  - ").append(summarizeElementMap(n)).append("\n");
				}
			}
			sb.append("\n");
		}
		catch (Exception e) {
			log.warn("[JanusGraph] Failed building inventory topology section for nodeId={}", nodeId, e);
			sb.append("- **Error:** ").append(e.getMessage()).append("\n\n");
		}
	}

	/** Under an inventory child (usually an interface), list direct OSPF adjacencies on that vertex. */
	private void appendOspfPeersIndented(StringBuilder sb, String vertexId, int maxPeers) {
		try {
			GraphTraversal<?, ?> trav = optionalLongLimit(
					g.V().has("nodeId", vertexId).both("CONNECTED_TO").dedup().values("nodeId"), maxPeers);
			LinkedHashSet<String> peers = new LinkedHashSet<>();
			trav.forEachRemaining(v -> {
				String p = Objects.toString(v, "").trim();
				if (!p.isEmpty() && !vertexId.equals(p)) {
					peers.add(p);
				}
			});
			if (peers.isEmpty()) {
				return;
			}
			sb.append("    - **OSPF `CONNECTED_TO` peers:** ");
			sb.append(peers.stream().map(x -> "`" + x + "`").collect(Collectors.joining(", ")));
			sb.append("\n");
		}
		catch (Exception e) {
			log.debug("[JanusGraph] OSPF peers for vertexId={}: {}", vertexId, e.getMessage());
		}
	}

	/** One-line human summary of a vertex property map. */
	private static String summarizeElementMap(Map<String, Object> m) {
		String id = str(m, "nodeId", "UNKNOWN");
		String nm = str(m, "name", id);
		String neType = str(m, "neType", "—");
		String lvl = str(m, "hierarchyLevel", "—");
		String vendor = str(m, "vendor", "—");
		String tech = str(m, "technology", "—");
		String ipv4 = str(m, "ipv4", "");
		boolean hasIp = ipv4 != null && !ipv4.isBlank() && !"UNKNOWN".equalsIgnoreCase(ipv4);
		String ipPart = hasIp ? " | ipv4=`" + ipv4 + "`" : "";
		return String.format("`%s` (%s) | type=%s | level=%s | vendor=%s | tech=%s%s",
				id, nm, neType, lvl, vendor, tech, ipPart);
	}

	// -------------------------------------------------------------------------
	// Edge case handlers
	// -------------------------------------------------------------------------

	private RcaResult handleIsolatedSingleNode(String nodeId) {
		RcaGroup group = buildIsolatedLeafGroup(nodeId);
		return new RcaResult(
				List.of(nodeId),
				List.of(group),
				false,
				"Single alarming node — isolated leaf failure.");
	}

	private RcaResult handleNoCommonAncestors(List<String> nodeIds) {
		List<RcaGroup> groups = nodeIds.stream()
				.map(this::buildIsolatedLeafGroup)
				.collect(Collectors.toList());
		return new RcaResult(nodeIds, groups, true,
				"No common ancestor found — nodes may be top-level or unconnected in the graph. "
				+ "Ensure inventory data has been loaded into JanusGraph.");
	}

	// -------------------------------------------------------------------------
	// Property extraction utilities
	// -------------------------------------------------------------------------

	private static String str(Map<String, Object> map, String key) {
		Object v = map.get(key);
		return v != null ? v.toString() : "UNKNOWN";
	}

	private static String str(Map<String, Object> map, String key, String defaultVal) {
		Object v = map.get(key);
		return v != null ? v.toString() : defaultVal;
	}

	private static long longVal(Map<String, Object> map, String key) {
		Object v = map.get(key);
		if (v instanceof Number n) return n.longValue();
		if (v instanceof String s) {
			try {
				return Long.parseLong(s);
			}
		catch (NumberFormatException ignored) {
			// Non-numeric string — fall through to return 0L
		}
		}
		return 0L;
	}

}
