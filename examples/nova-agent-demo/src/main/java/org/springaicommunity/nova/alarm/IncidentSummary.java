package org.springaicommunity.nova.alarm;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public final class IncidentSummary {

	public final String entityName;
	public final String alarmCode;
	public final String subentity;
	public final String probableCause;
	public final int rowCount;
	public final Map<String, Integer> severityCounts;
	public final Instant firstOpenTime;
	public final Instant lastChangeTime;

	IncidentSummary(String entityName, String alarmCode, String subentity, String probableCause,
			int rowCount, Map<String, Integer> severityCounts, Instant firstOpenTime, Instant lastChangeTime) {
		this.entityName = entityName;
		this.alarmCode = alarmCode;
		this.subentity = subentity;
		this.probableCause = probableCause;
		this.rowCount = rowCount;
		this.severityCounts = new LinkedHashMap<>(severityCounts);
		this.firstOpenTime = firstOpenTime;
		this.lastChangeTime = lastChangeTime;
	}
}

