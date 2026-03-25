package org.springaicommunity.nova.alarm;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class AlarmFlapAnalyzer {

	private AlarmFlapAnalyzer() {
	}

	public static String formatFlapSignals(List<AlarmRow> rows, int minChangeEvents, int topN) {
		if (rows == null || rows.isEmpty()) {
			return "### Flap signals\n\n_(no alarm rows to analyze)_\n";
		}
		int threshold = Math.max(2, minChangeEvents);

		Map<Key, Mutable> byKey = new LinkedHashMap<>();
		for (AlarmRow r : rows) {
			String entity = r.entityName();
			if (entity.isBlank()) continue;
			Key k = new Key(entity, r.alarmCode(), r.subentity());
			Mutable m = byKey.computeIfAbsent(k, kk -> new Mutable(entity, r.alarmCode(), r.subentity()));
			m.rows++;
			Instant ct = parseInstantBestEffort(r.changeTime());
			if (ct != null) {
				m.distinctChangeMinuteBuckets.add(bucketMinute(ct));
			}
		}

		List<Mutable> candidates = new ArrayList<>();
		for (Mutable m : byKey.values()) {
			int changeBuckets = m.distinctChangeMinuteBuckets.size();
			if (changeBuckets >= threshold) {
				m.changeBuckets = changeBuckets;
				candidates.add(m);
			}
		}

		candidates.sort(Comparator
				.comparingInt((Mutable m) -> m.changeBuckets).reversed()
				.thenComparingInt(m -> m.rows).reversed()
				.thenComparing(m -> m.entityName));

		StringBuilder sb = new StringBuilder();
		sb.append("### Flap signals\n\n");
		sb.append("_Heuristic: (ENTITY_NAME, ALARM_CODE, SUBENTITY) with distinct CHANGE_TIME minute buckets ≥ ")
				.append(threshold).append("._\n\n");

		if (candidates.isEmpty()) {
			sb.append("_(no strong flap candidates detected by this heuristic)_\n");
			return sb.toString();
		}

		int n = Math.max(0, Math.min(topN, candidates.size()));
		sb.append("| # | ENTITY_NAME | ALARM_CODE | SUBENTITY | changeBuckets | rows |\n");
		sb.append("|---:|---|---:|---|---:|---:|\n");
		for (int i = 0; i < n; i++) {
			Mutable m = candidates.get(i);
			sb.append("| ").append(i + 1).append(" | ")
					.append(escapePipe(m.entityName)).append(" | ")
					.append(escapePipe(m.alarmCode)).append(" | ")
					.append(escapePipe(m.subentity)).append(" | ")
					.append(m.changeBuckets).append(" | ")
					.append(m.rows).append(" |\n");
		}
		if (candidates.size() > n) {
			sb.append("\n_").append(candidates.size() - n).append(" more flap candidates omitted (topN=").append(n).append(")._ \n");
		}
		return sb.toString();
	}

	private static Instant parseInstantBestEffort(String s) {
		if (s == null || s.isBlank()) return null;
		try {
			return Instant.parse(s.trim());
		}
		catch (Exception ignored) {
			return null;
		}
	}

	private static String bucketMinute(Instant t) {
		long epochMin = t.getEpochSecond() / 60L;
		return Long.toString(epochMin);
	}

	private static String escapePipe(String s) {
		String t = Objects.toString(s, "").trim();
		return t.replace("|", "\\|");
	}

	private record Key(String entityName, String alarmCode, String subentity) {
		Key {
			entityName = norm(entityName);
			alarmCode = norm(alarmCode);
			subentity = norm(subentity);
		}

		private static String norm(String s) {
			return s == null ? "" : s.trim().toUpperCase(Locale.ROOT);
		}
	}

	private static final class Mutable {
		final String entityName;
		final String alarmCode;
		final String subentity;
		int rows;
		int changeBuckets;
		final java.util.LinkedHashSet<String> distinctChangeMinuteBuckets = new java.util.LinkedHashSet<>();

		Mutable(String entityName, String alarmCode, String subentity) {
			this.entityName = entityName == null ? "" : entityName.trim();
			this.alarmCode = alarmCode == null ? "" : alarmCode.trim();
			this.subentity = subentity == null ? "" : subentity.trim();
		}
	}
}

