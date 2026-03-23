package org.springaicommunity.nova;

import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Shared console state and inter-tool data cache for the NOVA CLI.
 *
 * <h3>Tool progress tracking</h3>
 * Tools call {@link #toolStarted(String)} / {@link #toolFinished()} so the
 * progress ticker in {@link NovaApplication} can show which sub-agent is active.
 *
 * <h3>Data cache</h3>
 * Large payloads (e.g. enriched PM JSON) are stored here instead of being
 * passed directly as tool-call parameters.  This avoids hitting the LLM's
 * function-parameter size limit.
 *
 * <p>Workflow:
 * <ol>
 *   <li>{@code PmDataFetchTool} calls {@link #putData(String)} → gets back a
 *       short {@code DATA_REF:nnn} key and returns that key to NOVA.</li>
 *   <li>NOVA passes the key string to {@code PmAnalystTool}.</li>
 *   <li>{@code PmAnalystTool} calls {@link #resolveData(String)} which returns
 *       the original JSON if the value starts with {@code DATA_REF:},
 *       otherwise returns the value as-is (so callers never need to pre-check).</li>
 * </ol>
 *
 * <p>Entries are kept forever within a single JVM session (NOC shift), which is
 * fine for a CLI tool. For a long-running server, add TTL expiry as needed.
 */
public final class AgentConsole {

	/** Human-readable name of the tool currently running, or blank if none. */
	public static final AtomicReference<String> currentTool = new AtomicReference<>("");

	/**
	 * Per-turn tool call history for logging/traceability.
	 *
	 * <p>This CLI is single-user and processes one request at a time, so using
	 * a simple shared list is sufficient.
	 */
	private static final Object TURN_LOCK = new Object();
	private static final List<String> turnToolCalls = new ArrayList<>();

	/**
	 * Tracks whether we already collected JanusGraph hierarchy evidence for the
	 * current user turn (either via NetworkTopologyRca tool or internally).
	 */
	private static final AtomicBoolean janusGraphEvidenceCalledThisTurn = new AtomicBoolean(false);

	private static final Map<String, String> DATA_CACHE = new ConcurrentHashMap<>();
	private static final AtomicLong COUNTER = new AtomicLong(0);
	private static final String REF_PREFIX = "DATA_REF:";

	private AgentConsole() {
	}

	// -------------------------------------------------------------------------
	// Tool progress
	// -------------------------------------------------------------------------

	public static void toolStarted(String toolName) {
		currentTool.set(toolName);
		if (toolName != null && !toolName.isBlank()) {
			synchronized (TURN_LOCK) {
				turnToolCalls.add(toolName);
			}
		}
	}

	public static void toolFinished() {
		currentTool.set("");
	}

	/** Clears per-turn tool history. Call before invoking NOVA for a request. */
	public static void beginToolTracking() {
		synchronized (TURN_LOCK) {
			turnToolCalls.clear();
		}
		janusGraphEvidenceCalledThisTurn.set(false);
	}

	/** Mark that JanusGraph hierarchy evidence has been collected for this turn. */
	public static void markJanusGraphEvidenceCalled() {
		janusGraphEvidenceCalledThisTurn.set(true);
	}

	/** Returns whether JanusGraph evidence was collected for this turn. */
	public static boolean isJanusGraphEvidenceCalled() {
		return janusGraphEvidenceCalledThisTurn.get();
	}

	/**
	 * Returns tool call history captured since {@link #beginToolTracking()},
	 * and clears it for the next turn.
	 */
	public static List<String> endToolTracking() {
		synchronized (TURN_LOCK) {
			if (turnToolCalls.isEmpty()) {
				return Collections.emptyList();
			}
			List<String> copy = List.copyOf(turnToolCalls);
			turnToolCalls.clear();
			return copy;
		}
	}

	// -------------------------------------------------------------------------
	// Data cache — keeps large payloads out of LLM tool-call parameters
	// -------------------------------------------------------------------------

	/**
	 * Stores {@code data} in the cache and returns a compact reference key
	 * of the form {@code DATA_REF:nnn} that can be safely passed to other tools.
	 */
	public static String putData(String data) {
		String key = REF_PREFIX + COUNTER.incrementAndGet();
		DATA_CACHE.put(key, data);
		return key;
	}

	/**
	 * If {@code value} is a {@code DATA_REF:nnn} key, returns the cached data.
	 * Otherwise returns {@code value} unchanged (already the real payload).
	 * Returns {@code null} if the key is not found in the cache.
	 */
	public static String resolveData(String value) {
		if (value != null && value.startsWith(REF_PREFIX)) {
			return DATA_CACHE.get(value);
		}
		return value;
	}

}
