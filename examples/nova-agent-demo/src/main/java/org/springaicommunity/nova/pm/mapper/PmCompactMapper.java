package org.springaicommunity.nova.pm.mapper;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.springaicommunity.nova.pm.dto.PmDataCompactResponse;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse.DataEntry;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse.Location;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse.Summary;
import org.springaicommunity.nova.pm.dto.PmDataPoint;
import org.springaicommunity.nova.pm.dto.PmDataResponse;
import org.springframework.stereotype.Component;

/**
 * Converts a {@link PmDataResponse} into the token-efficient
 * {@link PmDataCompactResponse} format.
 *
 * <p>Compression strategy:
 * <ol>
 *   <li><b>Summary</b> — node label, type, vendor, domain, period, and total
 *       point count are extracted once from the first data point's metadata.</li>
 *   <li><b>Location</b> — geographic hierarchy (L1 → region, L2 → state,
 *       L3 → city, L4 → site) lifted from metajson.</li>
 *   <li><b>Static values</b> — KPIs whose numeric value is identical (or null)
 *       across every time point are hoisted into a single {@code staticValues}
 *       map. They are omitted from each {@link DataEntry} to avoid repetition.</li>
 *   <li><b>Timestamps</b> — formatted as {@code yyyy-MM-dd'T'HH:mm} (UTC,
 *       seconds dropped) for brevity.</li>
 * </ol>
 */
@Component
public class PmCompactMapper {

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm").withZone(ZoneOffset.UTC);

    /**
     * Converts a full PM data response to the compact form.
     *
     * @param full response from {@link org.springaicommunity.nova.pm.service.PmDataService}
     * @return compact representation suitable for LLM consumption
     */
    public PmDataCompactResponse toCompact(PmDataResponse full) {
        List<PmDataPoint> points = full.getData();

        PmDataCompactResponse compact = new PmDataCompactResponse();
        compact.setSummary(buildSummary(full, points));
        compact.setLocation(buildLocation(points));

        // Collect every unique KPI code seen across all points
        List<String> allCodes = collectAllCodes(points);

        // Determine which KPIs are static (same value on every point)
        Map<String, Double> staticValues = detectStaticKpis(points, allCodes);

        // Build per-point data entries, excluding static KPIs
        List<DataEntry> data = buildDataEntries(points, staticValues.keySet());

        compact.setData(data);
        compact.setStaticValues(staticValues.isEmpty() ? null : new TreeMap<>(staticValues));
        return compact;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private Summary buildSummary(PmDataResponse full, List<PmDataPoint> points) {
        Summary s = new Summary();

        // Pull entity name from first point's meta if available
        String entityName = firstMeta(points, "ENTITY_NAME");
        String nodeName = full.getNodeName();
        s.setNode(entityName != null ? entityName + " (" + nodeName + ")" : nodeName);

        // NODE_TYPE preferred; fall back to networkType on the record itself
        String nodeType = firstMeta(points, "NODE_TYPE");
        if (nodeType == null) {
            nodeType = points.isEmpty() ? null : points.get(0).getNetworkType();
        }
        s.setType(nodeType);

        s.setVendor(full.getVendor());
        s.setDomain(full.getDomain());
        s.setTechnology(full.getTechnology());
        s.setGranularity(full.getGranularity() != null ? full.getGranularity().name() : null);

        if (full.getTimeRange() != null) {
            String from = TS_FMT.format(full.getTimeRange().getFrom());
            String to   = TS_FMT.format(full.getTimeRange().getTo());
            s.setPeriod(from + " to " + to);
        }

        s.setTotalPoints(full.getTotalPoints());
        return s;
    }

    private Location buildLocation(List<PmDataPoint> points) {
        Location loc = new Location();
        loc.setRegion(firstMeta(points, "L1"));
        loc.setState(firstMeta(points, "L2"));
        loc.setCity(firstMeta(points, "L3"));
        String l4 = firstMeta(points, "L4");
        if (l4 != null && !l4.isBlank()) {
            loc.setSite(l4);
        }
        return loc;
    }

    /** Returns the value of {@code key} from the first point that has it, or null. */
    private String firstMeta(List<PmDataPoint> points, String key) {
        for (PmDataPoint p : points) {
            if (p.getMeta() != null) {
                String v = p.getMeta().get(key);
                if (v != null && !v.isBlank()) return v;
            }
        }
        return null;
    }

    /** Collects every KPI code seen in any data point (insertion order preserved). */
    private List<String> collectAllCodes(List<PmDataPoint> points) {
        LinkedHashMap<String, Void> codes = new LinkedHashMap<>();
        for (PmDataPoint p : points) {
            if (p.getKpis() != null) {
                p.getKpis().forEach((code, v) -> codes.put(code, null));
            }
        }
        return new ArrayList<>(codes.keySet());
    }

    /**
     * Identifies KPIs whose value is the same (or always null) across all points.
     * A code is static only if it appears in every point with an identical value.
     *
     * @return sorted map of static KPI code → value (nulls excluded)
     */
    private Map<String, Double> detectStaticKpis(List<PmDataPoint> points, List<String> allCodes) {
        if (points.isEmpty()) return Map.of();

        Map<String, Double> statics = new LinkedHashMap<>();

        for (String code : allCodes) {
            Double firstValue = null;
            boolean isFirst = true;
            boolean allSame = true;

            for (PmDataPoint p : points) {
                Double val = (p.getKpis() != null) ? p.getKpis().get(code) : null;
                if (isFirst) {
                    firstValue = val;
                    isFirst = false;
                } else if (!Objects.equals(val, firstValue)) {
                    allSame = false;
                    break;
                }
            }

            // Only hoist if all points agree AND the value is non-null
            if (allSame && firstValue != null) {
                statics.put(code, firstValue);
            }
        }
        return statics;
    }

    /**
     * Builds the time-series data entries, omitting any KPI whose code is in
     * {@code staticCodes}.
     */
    private List<DataEntry> buildDataEntries(List<PmDataPoint> points,
            java.util.Set<String> staticCodes) {
        List<DataEntry> entries = new ArrayList<>(points.size());
        for (PmDataPoint p : points) {
            String time = p.getTimestamp() != null ? TS_FMT.format(p.getTimestamp()) : null;

            Map<String, Double> kpis = new LinkedHashMap<>();
            if (p.getKpis() != null) {
                p.getKpis().forEach((code, val) -> {
                    if (!staticCodes.contains(code)) {
                        kpis.put(code, val);
                    }
                });
            }
            entries.add(new DataEntry(time, kpis.isEmpty() ? null : kpis));
        }
        return entries;
    }
}
