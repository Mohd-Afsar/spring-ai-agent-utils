package org.springaicommunity.nova.alarm;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class AlarmRow {

	private final Map<String, String> byNormalizedHeader;

	public AlarmRow(Map<String, String> byNormalizedHeader) {
		this.byNormalizedHeader = Objects.requireNonNull(byNormalizedHeader, "byNormalizedHeader must not be null");
	}

	public String get(String header) {
		if (header == null) return "";
		String key = normalizeHeader(header);
		return byNormalizedHeader.getOrDefault(key, "");
	}

	public String entityName() {
		return get("ENTITY_NAME");
	}

	public String alarmCode() {
		return get("ALARM_CODE");
	}

	public String severity() {
		return get("SEVERITY");
	}

	public String alarmStatus() {
		return get("ALARM_STATUS");
	}

	public String openTime() {
		return get("OPEN_TIME");
	}

	public String changeTime() {
		return get("CHANGE_TIME");
	}

	public String probableCause() {
		return get("PROBABLE_CAUSE");
	}

	public String subentity() {
		return get("SUBENTITY");
	}

	public String geographyL1() {
		return get("GEOGRAPHY_L1_NAME");
	}

	public String geographyL2() {
		return get("GEOGRAPHY_L2_NAME");
	}

	public String geographyL3() {
		return get("GEOGRAPHY_L3_NAME");
	}

	static String normalizeHeader(String s) {
		if (s == null) return "";
		return s.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
	}
}

