package org.springaicommunity.nova.graph.sync;

import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Loads railtel {@code NETWORK_ELEMENT} (and OSPF topology) into JanusGraph using the
 * schema expected by {@code GraphTopologyService}:
 * <ul>
 *   <li>Vertex property {@code nodeId} = {@code NETWORK_ELEMENT.NE_NAME} (unique)</li>
 *   <li>Edge {@code PARENT_OF}: parent equipment → child (e.g. router → interface)</li>
 *   <li>Edge {@code CONNECTED_TO}: interface → interface (from {@code OSPF_LINK})</li>
 * </ul>
 */
@Service
@ConditionalOnBean(GraphTraversalSource.class)
public class JanusGraphInventorySyncService {

	private static final Logger log = LoggerFactory.getLogger(JanusGraphInventorySyncService.class);

	private final GraphTraversalSource g;
	private final JdbcTemplate jdbc;

	/**
	 * Skip sync only when graph has at least this fraction of MySQL NE rows (by nodeId count).
	 * Prevents a single stray vertex (e.g. old API test) from blocking a full load when
	 * {@code skip-if-nonempty=true}.
	 */
	@Value("${janusgraph.inventory-sync.min-completion-ratio:0.95}")
	private double minCompletionRatio;

	/** Log a progress line every N vertices/rows (0 = only phase start/end). */
	@Value("${janusgraph.inventory-sync.log-progress-every:3000}")
	private int logProgressEvery;

	public JanusGraphInventorySyncService(GraphTraversalSource g, DataSource dataSource) {
		this.g = g;
		this.jdbc = new JdbcTemplate(dataSource);
	}

	public record SyncStats(long verticesUpserted, long parentEdgesCreated, long connectedToEdgesCreated, long skippedRows) {}

	/**
	 * @param dropGraphBeforeSync if true, runs {@code g.V().drop()} first (destructive)
	 * @param loadOspfLinks       if true, create CONNECTED_TO from {@code OSPF_LINK}
	 * @param skipIfVerticesExist if true and the graph already has a <em>complete</em> inventory
	 *                            (see {@link #minCompletionRatio}), no-op unless force
	 * @param force               if true, run even when graph looks complete
	 */
	public SyncStats syncFromMysql(boolean dropGraphBeforeSync, boolean loadOspfLinks,
			boolean skipIfVerticesExist, boolean force) {

		long t0 = System.currentTimeMillis();

		long totalVertices = vertexCount();
		long mysqlNeRows = countMysqlNetworkElements();
		long graphWithNodeId = countVerticesWithNodeId();
		boolean inventoryLooksComplete = mysqlNeRows > 0
				&& graphWithNodeId >= (long) Math.floor(mysqlNeRows * minCompletionRatio);

		if (skipIfVerticesExist && inventoryLooksComplete && !force && !dropGraphBeforeSync) {
			log.info("[JanusSync] Skipping — inventory looks loaded: {} vertices with nodeId vs {} NE rows in MySQL (need ≥{}% ≈ {}). "
							+ "Use force=true or drop-graph-before-sync=true to rebuild.",
					graphWithNodeId, mysqlNeRows, (int) (minCompletionRatio * 100),
					(long) Math.ceil(mysqlNeRows * minCompletionRatio));
			return new SyncStats(0, 0, 0, 0);
		}

		if (!inventoryLooksComplete && totalVertices > 0) {
			log.warn("[JanusSync] Graph incomplete or stale: totalVertices={}, withNodeId={}, MySQL NE rows={}. Running sync.",
					totalVertices, graphWithNodeId, mysqlNeRows);
		}

		if (dropGraphBeforeSync) {
			log.warn("[JanusSync] Dropping all vertices (janusgraph.inventory-sync.drop-graph-before-sync=true) …");
			g.V().drop().iterate();
		}

		Map<Integer, String> idToName = new ConcurrentHashMap<>(65_536);
		long skipped = 0;
		long vertices = 0;

		var neRows = jdbc.query(
				"""
						SELECT ID, NE_NAME, NE_TYPE, NE_CATEGORY, TECHNOLOGY, VENDOR, DOMAIN,
						       FRIENDLY_NAME, HOST_NAME, PARENT_NE_ID_FK, IPV4
						FROM NETWORK_ELEMENT
						WHERE IFNULL(IS_DELETED, 0) = 0
						""",
				(rs, rowNum) -> new NeRow(
						rs.getInt("ID"),
						rs.getString("NE_NAME"),
						rs.getString("NE_TYPE"),
						rs.getString("NE_CATEGORY"),
						rs.getString("TECHNOLOGY"),
						rs.getString("VENDOR"),
						rs.getString("DOMAIN"),
						rs.getString("FRIENDLY_NAME"),
						rs.getString("HOST_NAME"),
						rs.getObject("PARENT_NE_ID_FK") == null ? null : rs.getInt("PARENT_NE_ID_FK"),
						rs.getString("IPV4")));

		for (NeRow row : neRows) {
			if (row.neName() == null || row.neName().isBlank()) {
				skipped++;
				continue;
			}
			idToName.put(row.id(), row.neName());
		}

		long totalValid = neRows.size() - skipped;
		log.info("[JanusSync] Phase 1/3 — upserting {} vertices (one Gremlin round-trip each; slow). {}",
				totalValid,
				logProgressEvery > 0
						? "Progress every " + logProgressEvery + " vertices."
						: "Progress log only at end (set janusgraph.inventory-sync.log-progress-every>0 for heartbeat).");

		long phase1Start = System.currentTimeMillis();
		for (NeRow row : neRows) {
			if (row.neName() == null || row.neName().isBlank()) {
				continue;
			}
			upsertVertex(row);
			vertices++;
			if (logProgressEvery > 0 && (vertices % logProgressEvery == 0 || vertices == totalValid)) {
				long elapsed = System.currentTimeMillis() - t0;
				double pct = totalValid > 0 ? (100.0 * vertices / totalValid) : 0.0;
				double vps = elapsed > 0 ? (vertices * 1000.0 / elapsed) : 0.0;
				log.info("[JanusSync] Phase 1/3 vertices: {} / {} ({}) — {} ms elapsed, ~{} verts/s",
						vertices, totalValid, String.format("%.1f%%", pct), elapsed, String.format("%.0f", vps));
			}
		}
		log.info("[JanusSync] Phase 1/3 done in {} ms.", System.currentTimeMillis() - phase1Start);

		long parentEdges = 0;
		log.info("[JanusSync] Phase 2/3 — adding PARENT_OF edges (router/switch → interface) …");
		long phase2Start = System.currentTimeMillis();
		long rowScan = 0;
		for (NeRow row : neRows) {
			rowScan++;
			if (logProgressEvery > 0 && rowScan % logProgressEvery == 0) {
				log.info("[JanusSync] Phase 2/3 scanned {} / {} NE rows, {} new PARENT_OF edges — {} ms",
						rowScan, neRows.size(), parentEdges, System.currentTimeMillis() - t0);
			}
			if (row.parentNeIdFk() == null || row.neName() == null || row.neName().isBlank()) {
				continue;
			}
			String parentName = idToName.get(row.parentNeIdFk());
			if (parentName == null || parentName.equals(row.neName())) {
				continue;
			}
			if (ensureParentEdge(parentName, row.neName())) {
				parentEdges++;
			}
		}
		log.info("[JanusSync] Phase 2/3 done in {} ms ({} PARENT_OF edges).", System.currentTimeMillis() - phase2Start, parentEdges);

		long linkEdges = 0;
		if (loadOspfLinks) {
			Set<String> seenPairs = new HashSet<>(4096);
			var links = jdbc.query(
					"""
							SELECT src.NE_NAME AS src_name, dst.NE_NAME AS dst_name
							FROM OSPF_LINK ol
							JOIN NETWORK_ELEMENT src ON src.ID = ol.SOURCE_INTERFACE_NE_ID
							JOIN NETWORK_ELEMENT dst ON dst.ID = ol.DESTINATION_INTERFACE_NE_ID
							WHERE IFNULL(ol.IS_DELETED, 0) = 0
							  AND IFNULL(src.IS_DELETED, 0) = 0
							  AND IFNULL(dst.IS_DELETED, 0) = 0
							  AND src.NE_NAME IS NOT NULL AND dst.NE_NAME IS NOT NULL
							""",
					(rs, i) -> new LinkRow(rs.getString("src_name"), rs.getString("dst_name")));

			log.info("[JanusSync] Phase 3/3 — adding CONNECTED_TO from OSPF ({} candidate links) …", links.size());
			long phase3Start = System.currentTimeMillis();
			long linkIdx = 0;
			for (LinkRow link : links) {
				linkIdx++;
				if (logProgressEvery > 0 && linkIdx % logProgressEvery == 0) {
					log.info("[JanusSync] Phase 3/3 OSPF links: processed {} / {}, {} CONNECTED_TO added — {} ms",
							linkIdx, links.size(), linkEdges, System.currentTimeMillis() - t0);
				}
				String a = link.src();
				String b = link.dst();
				if (a == null || b == null || a.isBlank() || b.isBlank() || a.equals(b)) {
					continue;
				}
				String k = a.compareTo(b) <= 0 ? a + "\u0001" + b : b + "\u0001" + a;
				if (!seenPairs.add(k)) {
					continue;
				}
				// One directed edge is enough for Gremlin both("CONNECTED_TO") from either end.
				if (ensureConnectedToEdge(a, b)) {
					linkEdges++;
				}
			}
			log.info("[JanusSync] Phase 3/3 done in {} ms ({} CONNECTED_TO edges).", System.currentTimeMillis() - phase3Start, linkEdges);
		}

		long elapsed = System.currentTimeMillis() - t0;
		long after = vertexCount();
		long afterNodeId = countVerticesWithNodeId();
		log.info("[JanusSync] Finished in {} ms — verticesUpserted≈{}, PARENT_OF edges added={}, CONNECTED_TO edges added={}, skippedRows={}, totalVertices after={}, withNodeId after={}",
				elapsed, vertices, parentEdges, linkEdges, skipped, after, afterNodeId);

		return new SyncStats(vertices, parentEdges, linkEdges, skipped);
	}

	private long vertexCount() {
		try {
			return g.V().count().next();
		}
		catch (Exception e) {
			log.warn("[JanusSync] vertexCount failed: {}", e.getMessage());
			return -1;
		}
	}

	private long countVerticesWithNodeId() {
		try {
			return g.V().has("nodeId").count().next();
		}
		catch (Exception e) {
			log.warn("[JanusSync] countVerticesWithNodeId failed: {}", e.getMessage());
			return -1;
		}
	}

	private long countMysqlNetworkElements() {
		Long n = jdbc.queryForObject(
				"""
						SELECT COUNT(*) FROM NETWORK_ELEMENT
						WHERE IFNULL(IS_DELETED, 0) = 0
						  AND NE_NAME IS NOT NULL AND TRIM(NE_NAME) <> ''
						""",
				Long.class);
		return n == null ? 0L : n;
	}

	private void upsertVertex(NeRow row) {
		String label = vertexLabel(row.neType());
		String displayName = pickName(row);
		String tech = emptyToUnknown(row.technology());
		String vendor = emptyToUnknown(row.vendor());
		String level = hierarchyLevel(row.neType());
		String neTypeVal = row.neType() == null ? "" : row.neType();
		String domainVal = row.domain() == null ? "" : row.domain();
		String ipv4Val = row.ipv4() == null ? "" : row.ipv4();

		// One round-trip per vertex: fold/coalesce is supported by Gremlin Server 3.6+
		g.V().has("nodeId", row.neName())
				.fold()
				.coalesce(
						__.unfold(),
						__.addV(label).property("nodeId", row.neName()))
				.property("name", displayName)
				.property("technology", tech)
				.property("vendor", vendor)
				.property("hierarchyLevel", level)
				.property("alarmCount", 0L)
				.property("neType", neTypeVal)
				.property("domain", domainVal)
				.property("ipv4", ipv4Val)
				.iterate();
	}

	private static String pickName(NeRow row) {
		if (row.hostName() != null && !row.hostName().isBlank()) {
			return row.hostName().trim();
		}
		if (row.friendlyName() != null && !row.friendlyName().isBlank()) {
			return row.friendlyName().trim();
		}
		return row.neName();
	}

	private static String emptyToUnknown(String s) {
		return (s == null || s.isBlank()) ? "UNKNOWN" : s.trim();
	}

	private static String hierarchyLevel(String neType) {
		if (neType == null) {
			return "UNKNOWN";
		}
		return switch (neType.toUpperCase(Locale.ROOT)) {
			case "INTERFACE" -> "INTERFACE";
			case "ROUTER", "SWITCH", "DWDM" -> "EQUIPMENT";
			default -> "EQUIPMENT";
		};
	}

	private static String vertexLabel(String neType) {
		if (neType == null) {
			return "NetworkElement";
		}
		return switch (neType.toUpperCase(Locale.ROOT)) {
			case "INTERFACE" -> "Interface";
			case "ROUTER" -> "Router";
			case "SWITCH" -> "Switch";
			case "DWDM" -> "Dwdm";
			default -> "NetworkElement";
		};
	}

	/** Parent --PARENT_OF--> child (matches GraphTopologyService traversals). */
	private boolean ensureParentEdge(String parentNodeId, String childNodeId) {
		try {
			if (!g.V().has("nodeId", parentNodeId).hasNext() || !g.V().has("nodeId", childNodeId).hasNext()) {
				return false;
			}
			if (g.V().has("nodeId", parentNodeId).out("PARENT_OF").has("nodeId", childNodeId).hasNext()) {
				return false;
			}
			g.V().has("nodeId", parentNodeId)
					.addE("PARENT_OF")
					.to(__.V().has("nodeId", childNodeId))
					.iterate();
			return true;
		}
		catch (Exception e) {
			log.debug("[JanusSync] PARENT_OF {} -> {} failed: {}", parentNodeId, childNodeId, e.getMessage());
			return false;
		}
	}

	private boolean ensureConnectedToEdge(String fromIf, String toIf) {
		try {
			if (!g.V().has("nodeId", fromIf).hasNext() || !g.V().has("nodeId", toIf).hasNext()) {
				return false;
			}
			if (g.V().has("nodeId", fromIf).out("CONNECTED_TO").has("nodeId", toIf).hasNext()) {
				return false;
			}
			g.V().has("nodeId", fromIf)
					.addE("CONNECTED_TO")
					.to(__.V().has("nodeId", toIf))
					.iterate();
			return true;
		}
		catch (Exception e) {
			log.debug("[JanusSync] CONNECTED_TO {} -> {} failed: {}", fromIf, toIf, e.getMessage());
			return false;
		}
	}

	private record NeRow(int id, String neName, String neType, String neCategory, String technology, String vendor,
			String domain, String friendlyName, String hostName, Integer parentNeIdFk, String ipv4) {
	}

	private record LinkRow(String src, String dst) {
	}
}
