package org.springaicommunity.nova.graph;

import java.util.List;

/**
 * Full root cause analysis result returned by {@link GraphTopologyService}.
 *
 * @param inputAlarmingNodes  the node IDs passed in by the caller
 * @param groups              one or more correlated fault groups; multiple
 *                            groups indicate independent simultaneous failures
 * @param multipleRootCauses  true when more than one independent root cause
 *                            was identified
 * @param executionNote       summary of what the algorithm found (or why it
 *                            could not complete normally)
 *
 * @author Spring AI Community
 */
public record RcaResult(
		List<String> inputAlarmingNodes,
		List<RcaGroup> groups,
		boolean multipleRootCauses,
		String executionNote) {

	/** Convenience factory for error / empty results. */
	public static RcaResult empty(String note) {
		return new RcaResult(List.of(), List.of(), false, note);
	}

}
