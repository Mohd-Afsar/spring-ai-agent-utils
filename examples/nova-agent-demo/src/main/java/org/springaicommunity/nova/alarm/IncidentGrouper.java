package org.springaicommunity.nova.alarm;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class IncidentGrouper {

	private IncidentGrouper() {
	}

	public static List<IncidentSummary> group(List<AlarmRow> rows) {
		if (rows == null || rows.isEmpty()) return List.of();

		Map<IncidentKey, Mutable> acc = new LinkedHashMap<>();
		for (AlarmRow r : rows) {
			String entity = r.entityName();
			if (entity.isBlank()) continue;
			IncidentKey key = new IncidentKey(entity, r.alarmCode(), r.subentity(), r.probableCause());
			Mutable m = acc.computeIfAbsent(key, k -> new Mutable(entity, r.alarmCode(), r.subentity(), r.probableCause()));
			m.rowCount++;
			m.bumpSeverity(r.severity());
			m.firstOpen = min(m.firstOpen, parseInstantBestEffort(r.openTime()));
			m.lastChange = max(m.lastChange, parseInstantBestEffort(r.changeTime()));
		}

		List<IncidentSummary> out = new ArrayList<>(acc.size());
		for (Mutable m : acc.values()) {
			out.add(new IncidentSummary(m.entityName, m.alarmCode, m.subentity, m.probableCause,
					m.rowCount, m.severityCounts, m.firstOpen, m.lastChange));
		}

		out.sort(Comparator
				.comparingInt((IncidentSummary s) -> s.rowCount).reversed()
				.thenComparing(s -> sevRank(s.severityCounts), Comparator.reverseOrder())
				.thenComparing(s -> s.entityName));
		return List.copyOf(out);
	}

	private static int sevRank(Map<String, Integer> counts) {
		if (counts == null || counts.isEmpty()) return 0;
		return counts.getOrDefault("CRITICAL", 0) * 1000
				+ counts.getOrDefault("MAJOR", 0) * 100
				+ counts.getOrDefault("MINOR", 0) * 10
				+ counts.getOrDefault("WARNING", 0);
	}

	private static Instant parseInstantBestEffort(String s) {
		if (s == null || s.isBlank()) return null;
		String t = s.trim();
		try {
			// Supports ISO-8601 already.
			return Instant.parse(t);
		}
		catch (Exception ignored) {
			// Best effort only (DB may store local timestamps). Keep null if unparseable.
			return null;
		}
	}

	private static Instant min(Instant a, Instant b) {
		if (a == null) return b;
		if (b == null) return a;
		return a.isBefore(b) ? a : b;
	}

	private static Instant max(Instant a, Instant b) {
		if (a == null) return b;
		if (b == null) return a;
		return a.isAfter(b) ? a : b;
	}

	private static final class Mutable {
		final String entityName;
		final String alarmCode;
		final String subentity;
		final String probableCause;
		int rowCount = 0;
		Instant firstOpen;
		Instant lastChange;
		final Map<String, Integer> severityCounts = new LinkedHashMap<>();

		Mutable(String entityName, String alarmCode, String subentity, String probableCause) {
			this.entityName = safe(entityName);
			this.alarmCode = safe(alarmCode);
			this.subentity = safe(subentity);
			this.probableCause = safe(probableCause);
		}

		void bumpSeverity(String sev) {
			String s = safe(sev).toUpperCase(Locale.ROOT);
			if (s.isBlank()) s = "UNKNOWN";
			severityCounts.put(s, severityCounts.getOrDefault(s, 0) + 1);
		}

		private static String safe(String s) {
			return s == null ? "" : s.trim();
		}
	}
}

