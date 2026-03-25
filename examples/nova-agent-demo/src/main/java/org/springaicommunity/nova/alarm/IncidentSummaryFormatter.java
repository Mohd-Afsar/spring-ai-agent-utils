package org.springaicommunity.nova.alarm;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class IncidentSummaryFormatter {

	private IncidentSummaryFormatter() {
	}

	public static String format(List<IncidentSummary> incidents, int topN) {
		List<IncidentSummary> list = incidents == null ? List.of() : incidents;
		int n = Math.max(0, Math.min(topN, list.size()));
		StringBuilder sb = new StringBuilder();
		sb.append("### Incident summary (grouped alarms)\n\n");
		sb.append("- Evidence policy: use only `ENTITY_NAME` / geography / codes present in the blocks below. Do not invent nodes/regions.\n\n");
		sb.append("| # | ENTITY_NAME | ALARM_CODE | SEVERITY (C/MJ/MN) | SUBENTITY | PROBABLE_CAUSE | rows |\n");
		sb.append("|---:|---|---:|---|---|---|---:|\n");
		for (int i = 0; i < n; i++) {
			IncidentSummary s = list.get(i);
			sb.append("| ").append(i + 1).append(" | ")
					.append(escapePipe(s.entityName)).append(" | ")
					.append(escapePipe(s.alarmCode)).append(" | ")
					.append(sevTriplet(s.severityCounts)).append(" | ")
					.append(escapePipe(s.subentity)).append(" | ")
					.append(escapePipe(trimTo(s.probableCause, 80))).append(" | ")
					.append(s.rowCount).append(" |\n");
		}
		if (list.size() > n) {
			sb.append("\n_").append(list.size() - n).append(" more incident groups omitted (topN=").append(n).append(")._ \n");
		}
		return sb.toString();
	}

	public static Set<String> allowedGeographyTokens(List<AlarmRow> rows) {
		if (rows == null || rows.isEmpty()) return Set.of();
		Set<String> out = new LinkedHashSet<>();
		for (AlarmRow r : rows) {
			addTokens(out, r.geographyL1());
			addTokens(out, r.geographyL2());
			addTokens(out, r.geographyL3());
		}
		return Set.copyOf(out);
	}

	private static void addTokens(Set<String> out, String value) {
		if (value == null) return;
		String t = value.trim();
		if (t.isBlank()) return;
		out.add(t);
		// also add words as weak tokens to detect hallucinations in output
		for (String part : t.split("\\s+")) {
			String p = part.trim();
			if (p.length() >= 3) out.add(p.toUpperCase(Locale.ROOT));
		}
	}

	private static String sevTriplet(Map<String, Integer> counts) {
		if (counts == null || counts.isEmpty()) return "0/0/0";
		int c = counts.getOrDefault("CRITICAL", 0);
		int mj = counts.getOrDefault("MAJOR", 0);
		int mn = counts.getOrDefault("MINOR", 0);
		return c + "/" + mj + "/" + mn;
	}

	private static String trimTo(String s, int max) {
		if (s == null) return "";
		String t = s.trim();
		if (t.length() <= max) return t;
		return t.substring(0, Math.max(0, max - 3)) + "...";
	}

	private static String escapePipe(String s) {
		String t = Objects.toString(s, "");
		if (t.isBlank()) return "";
		return t.replace("|", "\\|");
	}
}

