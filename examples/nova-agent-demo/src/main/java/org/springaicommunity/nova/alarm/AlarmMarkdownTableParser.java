package org.springaicommunity.nova.alarm;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class AlarmMarkdownTableParser {

	private AlarmMarkdownTableParser() {
	}

	public static List<AlarmRow> parse(String markdownTable) {
		if (markdownTable == null || markdownTable.isBlank()) return List.of();

		List<String> rows = new ArrayList<>();
		for (String line : markdownTable.split("\\r?\\n")) {
			String t = line.trim();
			if (t.startsWith("|") && t.endsWith("|")) {
				rows.add(t);
			}
		}
		if (rows.size() < 3) return List.of();

		List<String> headers = splitMarkdownRow(rows.get(0));
		List<String> normHeaders = new ArrayList<>(headers.size());
		for (String h : headers) {
			normHeaders.add(normalizeHeader(h));
		}

		List<AlarmRow> out = new ArrayList<>();
		for (int i = 2; i < rows.size(); i++) {
			String row = rows.get(i);
			if (row.contains("---")) continue;
			List<String> cells = splitMarkdownRow(row);
			if (cells.isEmpty()) continue;

			Map<String, String> map = new LinkedHashMap<>();
			for (int c = 0; c < normHeaders.size(); c++) {
				String key = normHeaders.get(c);
				if (key.isBlank()) continue;
				String val = c < cells.size() ? cleanCell(cells.get(c)) : "";
				map.put(key, val);
			}
			out.add(new AlarmRow(map));
		}
		return List.copyOf(out);
	}

	private static List<String> splitMarkdownRow(String row) {
		if (row == null || row.isBlank()) return List.of();
		String t = row.trim();
		if (!t.startsWith("|") || !t.endsWith("|")) return List.of();
		String core = t.substring(1, t.length() - 1);
		String[] parts = core.split("\\|", -1);
		List<String> out = new ArrayList<>(parts.length);
		for (String p : parts) out.add(p.trim());
		return out;
	}

	private static String cleanCell(String cell) {
		if (cell == null) return "";
		String t = cell.replace("**", "").trim();
		if ("null".equalsIgnoreCase(t)) return "";
		return t;
	}

	private static String normalizeHeader(String s) {
		if (s == null) return "";
		return s.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
	}
}

