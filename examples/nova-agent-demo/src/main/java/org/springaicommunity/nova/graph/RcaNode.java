package org.springaicommunity.nova.graph;

/**
 * Snapshot of a network element vertex retrieved from JanusGraph.
 *
 * <p>Properties are read from the vertex's element map. If a property is
 * absent in the graph, sensible defaults are used so callers never receive
 * null values.
 *
 * @param nodeId         unique inventory identifier stored on the vertex
 * @param name           human-readable node name
 * @param technology     radio/transport technology (2G / 3G / 4G / 5G / IP)
 * @param vendor         equipment vendor (Ericsson, Nokia, Huawei, …)
 * @param hierarchyLevel topology tier (REGION / SITE / CONTROLLER / CELL / CARRIER)
 * @param alarmCount     number of active alarms on this node (denormalised property)
 *
 * @author Spring AI Community
 */
public record RcaNode(
		String nodeId,
		String name,
		String technology,
		String vendor,
		String hierarchyLevel,
		long alarmCount) {

	/** Placeholder used when a node cannot be fetched from JanusGraph. */
	public static RcaNode unknown(String nodeId) {
		return new RcaNode(nodeId, nodeId, "UNKNOWN", "UNKNOWN", "UNKNOWN", 0L);
	}

}
