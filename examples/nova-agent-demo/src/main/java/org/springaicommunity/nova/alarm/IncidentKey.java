package org.springaicommunity.nova.alarm;

import java.util.Locale;
import java.util.Objects;

final class IncidentKey {

	final String entityName;
	final String alarmCode;
	final String subentity;
	final String probableCause;

	IncidentKey(String entityName, String alarmCode, String subentity, String probableCause) {
		this.entityName = norm(entityName);
		this.alarmCode = norm(alarmCode);
		this.subentity = norm(subentity);
		this.probableCause = norm(probableCause);
	}

	private static String norm(String s) {
		if (s == null) return "";
		return s.trim().toUpperCase(Locale.ROOT);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof IncidentKey other)) return false;
		return entityName.equals(other.entityName)
				&& alarmCode.equals(other.alarmCode)
				&& subentity.equals(other.subentity)
				&& probableCause.equals(other.probableCause);
	}

	@Override
	public int hashCode() {
		return Objects.hash(entityName, alarmCode, subentity, probableCause);
	}
}

