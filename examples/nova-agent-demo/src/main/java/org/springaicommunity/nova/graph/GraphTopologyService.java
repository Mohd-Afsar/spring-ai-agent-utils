package org.springaicommunity.nova.graph;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

	private final GraphTraversalSource g;

	/**
	 * Performs a one-time lightweight JanusGraph / Gremlin reachability check
	 * before running expensive RCA traversals.
	 */
	private final AtomicBoolean healthChecked = new AtomicBoolean(false);

	private volatile boolean janusGraphHealthy = false;

	public GraphTopologyService(GraphTraversalSource g) {
		this.g = g;
	}

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

	private boolean vertexExistsByNodeId(String nodeId) {
		return g.V().has("nodeId", nodeId).limit(1).values("nodeId").tryNext().isPresent();
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

	/** Must be >= {@code nova.topology.max-alarming-nodes} so the tool is not capped here first. */
	private static final int MAX_NODES_IN_INVENTORY_TOPOLOGY_REPORT = 256;

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
			int lim = Math.max(1, limit);
			List<Map<String, Object>> raw = withPlainTopologyProjection(
					g.V().has("nodeId", nodeId)
							.union(
									__.out("PARENT_OF").both("CONNECTED_TO").dedup().in("PARENT_OF"),
									__.both("CONNECTED_TO").in("PARENT_OF"))
							.dedup()
							.limit(lim * 3L))
					.toList();
			List<Map<String, Object>> out = new ArrayList<>();
			for (Map<String, Object> m : raw) {
				if (nodeId.equals(str(m, "nodeId"))) {
					continue;
				}
				out.add(m);
				if (out.size() >= lim) {
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

	public String formatInventoryTopologyReport(List<String> nodeIds, int maxChildrenPerNode, int maxNeighborsPerNode) {
		ensureJanusGraphHealth();
		if (!janusGraphHealthy) {
			return "## Network topology (inventory)\n\nJanusGraph is currently unavailable.";
		}
		if (nodeIds == null || nodeIds.isEmpty()) {
			return "## Network topology (inventory)\n\nNo nodes to resolve.";
		}
		List<String> distinct = nodeIds.stream()
				.filter(Objects::nonNull)
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.distinct()
				.limit(MAX_NODES_IN_INVENTORY_TOPOLOGY_REPORT)
				.toList();
		int maxCh = Math.max(1, Math.min(maxChildrenPerNode, 200));
		int maxNbr = Math.max(1, Math.min(maxNeighborsPerNode, 200));
		StringBuilder sb = new StringBuilder();
		sb.append("## Network topology (inventory)\n\n");
		sb.append("""
				_**PARENT_OF** — MySQL `NETWORK_ELEMENT.PARENT_NE_ID_FK`: parent equipment → child (often **router → interface**). \
				Routers with no parent row in CMDB show an empty parent chain; **children** are **interface** vertices only if those rows exist and `PARENT_NE_ID_FK` points here._

				_**CONNECTED_TO** — from **`OSPF_LINK`**: edges are created between **interface** `NE_NAME`s, not between routers. \
				**Direct** neighbors below are only non-empty if this vertex is an interface. \
				**Adjacent equipment (via OSPF)** walks: this NE → child interfaces → `CONNECTED_TO` → peer interface → parent equipment._

				""");
		sb.append("**NEs listed:** ").append(distinct.size()).append("\n\n---\n\n");
		for (String id : distinct) {
			appendInventoryNodeSection(sb, id, maxCh, maxNbr);
		}
		sb.append("""
				
				_Check **MySQL**: `NETWORK_ELEMENT` should include **INTERFACE** children with `PARENT_NE_ID_FK` set; `OSPF_LINK` should reference interface NE ids. \
				Run **JanusGraph inventory sync** with `janusgraph.inventory-sync.load-ospf-links=true`. Use `force=true` or `drop-graph-before-sync=true` if the graph was loaded before edges were fixed._
				""");
		return sb.toString();
	}

	private void appendInventoryNodeSection(StringBuilder sb, String nodeId, int maxCh, int maxNbr) {
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

			List<Map<String, Object>> children = withPlainTopologyProjection(
					g.V().has("nodeId", nodeId).out("PARENT_OF").limit(maxCh))
					.toList();
			long childTotal = g.V().has("nodeId", nodeId).out("PARENT_OF").count().tryNext().orElse(0L);
			sb.append("- **Children (out PARENT_OF), showing ").append(children.size());
			if (childTotal > children.size()) {
				sb.append(" of ").append(childTotal);
			}
			sb.append(":**\n");
			if (children.isEmpty()) {
				sb.append("  - _(none — no `PARENT_OF` children; for routers, add **INTERFACE** rows in MySQL with `PARENT_NE_ID_FK` → this NE’s id)_\n");
			}
			else {
				for (Map<String, Object> c : children) {
					sb.append("  - ").append(summarizeElementMap(c)).append("\n");
				}
			}

			List<Map<String, Object>> nbrsDirect = withPlainTopologyProjection(
					g.V().has("nodeId", nodeId).both("CONNECTED_TO").dedup().limit(maxNbr))
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
			sb.append("- **Adjacent equipment (OSPF via interfaces), showing ").append(nbrsViaOspf.size()).append(":**\n");
			if (nbrsViaOspf.isEmpty()) {
				sb.append("  - _(none — need interface children under this NE + `CONNECTED_TO` from sync, or this NE has no OSPF-linked paths)_\n");
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
