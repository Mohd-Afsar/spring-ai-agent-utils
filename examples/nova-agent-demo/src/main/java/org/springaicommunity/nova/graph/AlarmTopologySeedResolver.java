package org.springaicommunity.nova.graph;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import javax.sql.DataSource;

/**
 * Derives JanusGraph {@code nodeId} candidates from alarm query markdown and resolves them
 * against {@code NETWORK_ELEMENT} so topology traversals use the same NE_NAME keys as the graph.
 */
public final class AlarmTopologySeedResolver {

	private static final int MAX_SEEDS_FROM_MARKDOWN = 200;
	private static final int MAX_IN_CLAUSE = 80;

	private static final Pattern IPV4 = Pattern.compile(
			"^(?:25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)(?:\\.(?:25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)){3}$");

	/** KPI / OID blobs and key=value alarm extras — not inventory seeds. */
	private static final Pattern JUNK_TOPOLOGY_BLOB = Pattern.compile(
			"(?i)(lldpStats|ospfRouterId\\s*=|ospfIfIpAddress\\s*=|ospfAddressLessIf\\s*=)");

	private AlarmTopologySeedResolver() {
	}

	/**
	 * Column headers (normalized: lower alphanumeric only) that usually identify a network element
	 * or interface worth correlating with inventory.
	 */
	private static boolean headerIsTopologySeedSource(String normalizedHeader) {
		if (normalizedHeader == null || normalizedHeader.isEmpty()) {
			return false;
		}
		return normalizedHeader.contains("entityname")
				|| normalizedHeader.contains("entity")
				|| normalizedHeader.contains("nodename")
				|| normalizedHeader.contains("nodeid")
				|| normalizedHeader.contains("nename")
				|| normalizedHeader.contains("hostname")
				|| normalizedHeader.contains("host")
				|| normalizedHeader.contains("objectname")
				|| normalizedHeader.contains("alarmsource")
				|| normalizedHeader.contains("subentity")
				|| normalizedHeader.contains("location")
				|| normalizedHeader.contains("sitename")
				|| normalizedHeader.contains("site")
				|| normalizedHeader.contains("interface")
				|| normalizedHeader.contains("ifname")
				|| normalizedHeader.contains("portname")
				|| normalizedHeader.contains("ipaddress")
				|| normalizedHeader.contains("ipaddr")
				|| normalizedHeader.contains("neip")
				|| normalizedHeader.endsWith("ip");
	}

	/**
	 * Drops generic table values (NE types, status tokens) and SNMP/OID blobs that are not NE identifiers.
	 */
	static boolean isPlausibleTopologySeed(String value) {
		if (value == null) {
			return false;
		}
		String t = value.replace("**", "").trim();
		if (t.length() < 3 || t.length() > 480) {
			return false;
		}
		if ("null".equalsIgnoreCase(t)) {
			return false;
		}
		String u = t.toUpperCase(Locale.ROOT);
		if (u.equals("NA") || u.equals("N/A") || u.equals("UNKNOWN") || u.equals("ONAIR") || u.equals("OFFAIR")
				|| u.equals("BB") || u.equals("NONE")) {
			return false;
		}
		// Typical NE_TYPE / category column values — not hostnames
		if (u.equals("ROUTER") || u.equals("SWITCH") || u.equals("DWDM") || u.equals("INTERFACE")
				|| u.equals("SERVER") || u.equals("FIREWALL")) {
			return false;
		}
		if (JUNK_TOPOLOGY_BLOB.matcher(t).find()) {
			return false;
		}
		// Mostly datetime-like rows
		if (t.matches("^[\\d\\s:\\-+TZ.]+$") && t.matches(".*\\d{4}.*")) {
			return false;
		}
		return true;
	}

	private static String normalizeHeader(String s) {
		if (s == null) {
			return "";
		}
		return s.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
	}

	private static List<String> splitMarkdownRow(String row) {
		if (row == null || row.isBlank()) {
			return List.of();
		}
		String t = row.trim();
		if (!t.startsWith("|") || !t.endsWith("|")) {
			return List.of();
		}
		String core = t.substring(1, t.length() - 1);
		String[] parts = core.split(Pattern.quote("|"), -1);
		List<String> out = new ArrayList<>(parts.length);
		for (String part : parts) {
			out.add(part.trim());
		}
		return out;
	}

	/**
	 * Collect distinct non-empty cell values from every column whose header looks topology-relevant.
	 */
	public static List<String> extractSeedsFromAlarmMarkdown(String markdownTable) {
		if (markdownTable == null || markdownTable.isBlank()) {
			return List.of();
		}
		String[] lines = markdownTable.split("\\r?\\n");
		List<String> rows = new ArrayList<>();
		for (String line : lines) {
			String t = line.trim();
			if (t.startsWith("|") && t.endsWith("|")) {
				rows.add(t);
			}
		}
		if (rows.size() < 3) {
			return List.of();
		}
		List<String> headers = splitMarkdownRow(rows.get(0));
		if (headers.isEmpty()) {
			return List.of();
		}
		List<Integer> seedColumnIndexes = new ArrayList<>();
		for (int i = 0; i < headers.size(); i++) {
			String nh = normalizeHeader(headers.get(i));
			if (headerIsTopologySeedSource(nh)) {
				seedColumnIndexes.add(i);
			}
		}
		if (seedColumnIndexes.isEmpty()) {
			return List.of();
		}
		Set<String> distinct = new LinkedHashSet<>();
		for (int r = 2; r < rows.size(); r++) {
			String row = rows.get(r);
			if (row.contains("---")) {
				continue;
			}
			List<String> cells = splitMarkdownRow(row);
			for (int col : seedColumnIndexes) {
				if (col >= cells.size()) {
					continue;
				}
				String value = cells.get(col).replace("**", "").trim();
				if (value.isBlank() || "null".equalsIgnoreCase(value)) {
					continue;
				}
				if (!isPlausibleTopologySeed(value)) {
					continue;
				}
				distinct.add(value);
				if (distinct.size() >= MAX_SEEDS_FROM_MARKDOWN) {
					return List.copyOf(distinct);
				}
			}
		}
		return List.copyOf(distinct);
	}

	/**
	 * Map free-text alarm seeds to {@code NETWORK_ELEMENT.NE_NAME} rows (same string as Janus {@code nodeId}).
	 */
	public static List<String> resolveSeedsToNeNames(DataSource dataSource, Collection<String> seeds, int maxResults)
			throws SQLException {
		if (dataSource == null || seeds == null || seeds.isEmpty() || maxResults <= 0) {
			return List.of();
		}
		List<String> clean = new ArrayList<>();
		for (String s : seeds) {
			if (s == null) {
				continue;
			}
			String t = s.trim();
			if (t.isEmpty() || "null".equalsIgnoreCase(t)) {
				continue;
			}
			if (!isPlausibleTopologySeed(t)) {
				continue;
			}
			if (!clean.contains(t)) {
				clean.add(t);
			}
		}
		if (clean.isEmpty()) {
			return List.of();
		}
		LinkedHashSet<String> out = new LinkedHashSet<>();
		for (int i = 0; i < clean.size() && out.size() < maxResults; i += MAX_IN_CLAUSE) {
			int end = Math.min(i + MAX_IN_CLAUSE, clean.size());
			List<String> batch = clean.subList(i, end);
			resolveBatch(dataSource, batch, out, maxResults);
		}
		fuzzyResolveRemaining(dataSource, clean, out, 40, maxResults);
		return List.copyOf(out);
	}

	private static void resolveBatch(DataSource dataSource, List<String> batch, LinkedHashSet<String> out, int maxResults)
			throws SQLException {
		if (batch.isEmpty()) {
			return;
		}
		StringBuilder ph = new StringBuilder();
		for (int k = 0; k < batch.size(); k++) {
			if (k > 0) {
				ph.append(",");
			}
			ph.append("?");
		}
		String in = ph.toString();
		String sql = """
				SELECT DISTINCT NE_NAME FROM NETWORK_ELEMENT
				WHERE IFNULL(IS_DELETED, 0) = 0
				  AND NE_NAME IS NOT NULL AND TRIM(NE_NAME) <> ''
				  AND (
				    NE_NAME IN (%s) OR HOST_NAME IN (%s) OR FRIENDLY_NAME IN (%s) OR IPV4 IN (%s)
				  )
				""".formatted(in, in, in, in);
		try (Connection c = dataSource.getConnection();
				PreparedStatement ps = c.prepareStatement(sql)) {
			int p = 1;
			for (int pass = 0; pass < 4; pass++) {
				for (String s : batch) {
					ps.setString(p++, s);
				}
			}
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next() && out.size() < maxResults) {
					String name = rs.getString(1);
					if (name != null && !name.isBlank()) {
						out.add(name.trim());
					}
				}
			}
		}
	}

	/** Substring match on NE_NAME for interface-like tokens; capped to avoid query storms. */
	public static void fuzzyResolveRemaining(DataSource dataSource, Collection<String> seeds, LinkedHashSet<String> out,
			int maxExtraLookups, int maxResults) throws SQLException {
		if (dataSource == null || seeds == null || out.size() >= maxResults) {
			return;
		}
		int lookups = 0;
		for (String seed : seeds) {
			if (out.size() >= maxResults || lookups >= maxExtraLookups) {
				break;
			}
			if (seed == null || seed.length() < 3) {
				continue;
			}
			if (!(seed.contains("/") || seed.contains(":"))) {
				continue;
			}
			if (IPV4.matcher(seed).matches()) {
				continue;
			}
			lookups++;
			try (Connection c = dataSource.getConnection();
					PreparedStatement ps = c.prepareStatement(
							"""
									SELECT NE_NAME FROM NETWORK_ELEMENT
									WHERE IFNULL(IS_DELETED, 0) = 0
									  AND NE_NAME IS NOT NULL
									  AND NE_NAME LIKE CONCAT('%', ?, '%')
									LIMIT 5
									""")) {
				ps.setString(1, seed);
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next() && out.size() < maxResults) {
						String name = rs.getString(1);
						if (name != null && !name.isBlank()) {
							out.add(name.trim());
						}
					}
				}
			}
		}
	}

}
