package org.springaicommunity.nova.alarm;

import java.util.Locale;
import java.util.Set;

public final class AlarmEvidenceGuard {

	private AlarmEvidenceGuard() {
	}

	public static String appendGeographyDisclaimerIfNeeded(String llmOutput, Set<String> allowedGeographyTokens) {
		if (llmOutput == null || llmOutput.isBlank()) return llmOutput == null ? "" : llmOutput;
		if (allowedGeographyTokens == null || allowedGeographyTokens.isEmpty()) return llmOutput;

		String u = llmOutput.toUpperCase(Locale.ROOT);
		int hits = 0;
		for (String t : allowedGeographyTokens) {
			String tt = t == null ? "" : t.trim();
			if (tt.isBlank()) continue;
			if (u.contains(tt.toUpperCase(Locale.ROOT))) {
				hits++;
				if (hits >= 2) break;
			}
		}
		if (hits >= 1) {
			// Output includes some geography; assume ok.
			return llmOutput;
		}
		return llmOutput + "\n\n_Note: geography/region names were not present in the alarm evidence blocks for this run; verify any location claims against FM._";
	}
}

