package org.springaicommunity.nova.graph;

import java.util.List;

/**
 * One correlated alarm group produced by the graph-based RCA algorithm.
 *
 * <p>A single {@link RcaResult} may contain multiple groups when independent
 * failures are detected (pattern = {@link RcaPattern#MULTIPLE_ROOT_CAUSES}).
 *
 * @param rootCause      the node identified as the origin of this fault group
 * @param cascadedNodes  alarming nodes whose fault is attributed to rootCause
 * @param blastRadius    all downstream nodes that are at risk if rootCause is
 *                       not resolved (includes both alarming and healthy nodes)
 * @param pattern        structural pattern of this fault group
 * @param confidence     RCA confidence score in [0.0, 1.0]
 * @param coverageRatio  fraction of all alarming nodes covered by this group
 * @param reasoning      human-readable explanation of how the root cause was
 *                       determined (topology evidence + alarm evidence)
 *
 * @author Spring AI Community
 */
public record RcaGroup(
		RcaNode rootCause,
		List<RcaNode> cascadedNodes,
		List<RcaNode> blastRadius,
		RcaPattern pattern,
		double confidence,
		double coverageRatio,
		String reasoning) {
}
