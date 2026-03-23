package org.springaicommunity.nova.graph;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Collapses inventory / Janus {@code NE_NAME} values that encode child <strong>interface</strong> vertices
 * (e.g. {@code ROUTER_ge-0/1/0_510}) to the parent <strong>equipment</strong> name for compact topology reports.
 */
public final class TopologyNeNames {

	/**
	 * Underscore followed by typical Juniper-style interface / pseudo-interface tokens in synced NE_NAMEs.
	 */
	private static final Pattern INTERFACE_NE_SUFFIX = Pattern.compile(
			"_(?:ge-|fxp|lo|lt-|gr-|em|irb|dsc|lsi(?:\\.|_)|gre|tap|mtun|pimd|pime|ipip|vlan|re-|mt-|ae\\d)");

	private TopologyNeNames() {
	}

	/**
	 * If {@code neName} looks like {@code &lt;equipment&gt;_&lt;if-token&gt;…}, returns the equipment prefix; otherwise the input trimmed.
	 */
	public static String parentEquipmentNeName(String neName) {
		if (neName == null || neName.isBlank()) {
			return neName == null ? null : neName.trim();
		}
		String t = neName.trim();
		var m = INTERFACE_NE_SUFFIX.matcher(t);
		if (m.find()) {
			return t.substring(0, m.start());
		}
		return t;
	}

	/**
	 * Distinct parent equipment names, insertion order preserved, capped at {@code max}.
	 */
	public static List<String> distinctParentEquipment(Iterable<String> nodeIds, int max) {
		if (nodeIds == null || max <= 0) {
			return List.of();
		}
		LinkedHashSet<String> out = new LinkedHashSet<>();
		for (String id : nodeIds) {
			if (id == null) {
				continue;
			}
			String p = parentEquipmentNeName(id);
			if (p != null && !p.isEmpty()) {
				out.add(p);
			}
			if (out.size() >= max) {
				break;
			}
		}
		return List.copyOf(out);
	}
}
