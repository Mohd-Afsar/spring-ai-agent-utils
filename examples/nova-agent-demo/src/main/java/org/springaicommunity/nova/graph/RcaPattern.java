package org.springaicommunity.nova.graph;

/**
 * Classifies the structural pattern of a root cause group identified
 * during network topology RCA.
 *
 * <p>Pattern is determined by analysing graph traversal results —
 * ancestor coverage ratios, sibling alarm counts, and hierarchy depth.
 *
 * @author Spring AI Community
 */
public enum RcaPattern {

	/**
	 * One parent node failure cascades alarms to all or most children.
	 * Coverage ≥ 85 %, cascaded count ≥ 1.
	 * Example: RNC hardware fault → all cells under it alarm.
	 */
	SINGLE_CASCADE,

	/**
	 * Multiple sibling nodes share a common failing parent resource
	 * (power, backhaul, controller) even when the parent has no direct alarm.
	 * Indicated by high sibling-alarm ratio (≥ 60 %).
	 */
	SHARED_RESOURCE,

	/**
	 * Two or more independent failures with separate root causes.
	 * No single ancestor covers ≥ 85 % of all alarming nodes.
	 * Alarm groups are reported separately.
	 */
	MULTIPLE_ROOT_CAUSES,

	/**
	 * A single node is alarming with no cascade evidence and no common ancestor
	 * shared with other alarming nodes. The alarming node IS the root cause.
	 */
	ISOLATED_LEAF,

	/**
	 * Alarms are distributed along a transport path (CONNECTED_TO edges)
	 * rather than the PARENT_OF hierarchy, suggesting a link or transport failure
	 * between two hops.
	 */
	LINK_FAILURE,

	/**
	 * Some alarming nodes are part of a cascade from a shared root, while others
	 * appear to be independent. Coverage is moderate (50–84 %).
	 */
	PARTIAL_CASCADE

}
