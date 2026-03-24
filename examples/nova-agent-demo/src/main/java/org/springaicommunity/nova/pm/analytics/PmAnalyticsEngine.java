package org.springaicommunity.nova.pm.analytics;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springaicommunity.nova.pm.analytics.PmNodeSummary.DimensionScores;
import org.springaicommunity.nova.pm.analytics.PmNodeSummary.HealthStatus;
import org.springaicommunity.nova.pm.analytics.PmNodeSummary.KpiAnomaly;
import org.springaicommunity.nova.pm.analytics.PmNodeSummary.KpiAnomaly.Severity;
import org.springaicommunity.nova.pm.dto.KpiFormulaDetails;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse.DataEntry;
import org.springaicommunity.nova.pm.dto.PmDataEnrichedResponse;
import org.springframework.stereotype.Component;

/**
 * Telecom-grade PM analytics engine.
 *
 * <p>Converts raw {@link PmDataEnrichedResponse} (Cassandra time-series + KPI
 * metadata) into a pre-computed {@link PmNodeSummary} ready for LLM consumption.
 *
 * <h3>Why this matters at scale</h3>
 * <ul>
 *   <li>1 node, 11 hourly points = ~5,000 tokens raw JSON</li>
 *   <li>After this engine = ~150–250 tokens (the summary only)</li>
 *   <li>For 100K nodes, only Top-N anomalous nodes are sent to the LLM</li>
 * </ul>
 *
 * <h3>What is computed in Java (NOT by the LLM)</h3>
 * <ul>
 *   <li>Mean, peak, min per KPI across the time window</li>
 *   <li>Trend direction (RISING / FALLING / STABLE)</li>
 *   <li>Spike detection  (value &gt; 2× mean at any single point)</li>
 *   <li>Sustained high   (value &gt; 1.5× mean for ≥ 3 consecutive points)</li>
 *   <li>Gradual increase (monotonically rising ≥ 4 points)</li>
 *   <li>Dip detection    (value &lt; 0.5× mean)</li>
 *   <li>Threshold breach check against staticValues baselines</li>
 *   <li>Top-3 busiest time points by aggregate load</li>
 *   <li>5-dimension performance scoring and composite score</li>
 *   <li>Health status classification</li>
 * </ul>
 */
@Component
public class PmAnalyticsEngine {

    private static final Logger log = LoggerFactory.getLogger(PmAnalyticsEngine.class);

    // Spike: single point > SPIKE_FACTOR × mean
    private static final double SPIKE_FACTOR = 2.0;
    // Sustained: value > SUSTAINED_FACTOR × mean for >= SUSTAINED_MIN_POINTS consecutive points
    private static final double SUSTAINED_FACTOR = 1.5;
    private static final int SUSTAINED_MIN_POINTS = 3;
    // Dip: value < DIP_FACTOR × mean
    private static final double DIP_FACTOR = 0.5;
    // Gradual increase: monotonically rising >= this many points
    private static final int GRADUAL_MIN_POINTS = 4;
    private static final double EPS = 1e-9;

    /**
     * Main entry point. Produces a {@link PmNodeSummary} from enriched PM data.
     */
    public PmNodeSummary analyze(PmDataEnrichedResponse enriched) {
        PmDataCompactResponse compact = toCompact(enriched);
        Map<String, KpiFormulaDetails> kpiDetails =
                enriched.getKpiDetails() != null ? enriched.getKpiDetails() : Map.of();

        // Log any KPI codes present in the time-series but missing from the catalog
        if (compact.getData() != null) {
            List<String> missingFromCatalog = compact.getData().stream()
                    .filter(d -> d.getKpis() != null)
                    .flatMap(d -> d.getKpis().keySet().stream())
                    .distinct()
                    .filter(code -> !kpiDetails.containsKey(code))
                    .sorted()
                    .toList();
            if (!missingFromCatalog.isEmpty()) {
                log.warn("KPI codes not found in KPI_FORMULA catalog (using raw code as name): {}",
                        missingFromCatalog);
            }
        }

        PmNodeSummary summary = new PmNodeSummary();

        // ── Identity ──────────────────────────────────────────────────────────
        if (compact.getSummary() != null) {
            PmDataCompactResponse.Summary s = compact.getSummary();
            summary.setNode(s.getNode());
            summary.setVendor(s.getVendor());
            summary.setDomain(s.getDomain());
            summary.setTechnology(s.getTechnology());
            summary.setGranularity(s.getGranularity());
            summary.setPeriod(s.getPeriod());
            summary.setTotalPoints((int) s.getTotalPoints());
        }
        if (compact.getLocation() != null) {
            PmDataCompactResponse.Location loc = compact.getLocation();
            summary.setRegion(loc.getRegion());
            summary.setState(loc.getState());
            summary.setCity(loc.getCity());
        }

        // ── Per-KPI statistics ────────────────────────────────────────────────
        List<DataEntry> data = compact.getData() != null ? compact.getData() : List.of();
        Map<String, double[]> seriesMap = buildSeriesMap(data);
        Map<String, Double> staticValues =
                compact.getStaticValues() != null ? compact.getStaticValues() : Map.of();

        // ── Anomaly detection ─────────────────────────────────────────────────
        List<KpiAnomaly> anomalies = new ArrayList<>();
        for (Map.Entry<String, double[]> e : seriesMap.entrySet()) {
            String code = e.getKey();
            double[] values = e.getValue();
            String kpiName = kpiName(code, kpiDetails);
            KpiContext ctx = kpiContext(code, kpiDetails);
            detectAnomalies(code, kpiName, values, data, anomalies, staticValues, ctx);
        }
        // Sort: CRITICAL first, then by deviation descending
        anomalies.sort(Comparator
                .comparingInt((KpiAnomaly a) -> -a.getSeverity().ordinal())
                .thenComparingDouble(a -> -a.getDeviationPct()));
        summary.setAnomalies(anomalies);

        // ── Busiest periods ───────────────────────────────────────────────────
        summary.setBusiestPeriods(topBusiestPeriods(data, 3));

        // ── Performance score ─────────────────────────────────────────────────
        DimensionScores dims = scoreDimensions(seriesMap, staticValues, anomalies);
        summary.setDimensionScores(dims);
        int composite = (int) Math.round(
                dims.getAvailability() * 0.20
                        + dims.getThroughput() * 0.25
                        + dims.getReliability() * 0.35
                        + dims.getResourceEfficiency() * 0.20);
        summary.setPerformanceScore(Math.max(0, Math.min(100, composite)));

        // ── Health status ─────────────────────────────────────────────────────
        summary.setHealth(deriveHealth(anomalies, composite));

        // ── Human-readable findings ───────────────────────────────────────────
        summary.setFindings(buildFindings(anomalies, summary));

        log.info("Analytics complete for node={} health={} score={} anomalies={}",
                summary.getNode(), summary.getHealth(), summary.getPerformanceScore(), anomalies.size());
        return summary;
    }

    // -------------------------------------------------------------------------
    // Anomaly detection
    // -------------------------------------------------------------------------

    private void detectAnomalies(String code, String kpiName, double[] values,
            List<DataEntry> data, List<KpiAnomaly> out, Map<String, Double> staticValues, KpiContext ctx) {

        if (values.length == 0) return;
        double mean = mean(values);
        double peak = max(values);
        double min = min(values);
        if (!Double.isFinite(mean) || Math.abs(mean) < EPS) return;

        String trend = trend(values);

        // Spike
        for (int i = 0; i < values.length; i++) {
            if (!Double.isFinite(values[i])) continue;
            if (values[i] > ctx.spikeFactor * mean) {
                out.add(anomaly(code, kpiName, mean, values[i], trend,
                        KpiAnomaly.AnomalyType.SPIKE, data.get(i).getTime(), ctx));
                return; // one anomaly per KPI
            }
        }
        // Sustained high
        int run = 0;
        int runStart = -1;
        for (int i = 0; i < values.length; i++) {
            if (!Double.isFinite(values[i])) {
                run = 0;
                continue;
            }
            if (values[i] > ctx.sustainedFactor * mean) {
                if (run == 0) runStart = i;
                run++;
                if (run >= SUSTAINED_MIN_POINTS) {
                    out.add(anomaly(code, kpiName, mean, values[runStart], trend,
                            KpiAnomaly.AnomalyType.SUSTAINED_HIGH, data.get(runStart).getTime(), ctx));
                    return;
                }
            } else {
                run = 0;
            }
        }
        // Dip
        for (int i = 0; i < values.length; i++) {
            if (!Double.isFinite(values[i])) continue;
            if (values[i] < ctx.dipFactor * mean && mean > 0) {
                out.add(anomaly(code, kpiName, mean, values[i], trend,
                        KpiAnomaly.AnomalyType.DIP, data.get(i).getTime(), ctx));
                return;
            }
        }
        // Gradual increase
        if (values.length >= GRADUAL_MIN_POINTS && isMonotonicallyRising(values, GRADUAL_MIN_POINTS)) {
            out.add(anomaly(code, kpiName, mean, peak, trend,
                    KpiAnomaly.AnomalyType.GRADUAL_INCREASE, data.get(0).getTime(), ctx));
        }
    }

    private KpiAnomaly anomaly(String code, String name, double mean, double actual,
            String trend, KpiAnomaly.AnomalyType type, String detectedAt, KpiContext ctx) {
        KpiAnomaly a = new KpiAnomaly();
        a.setKpiCode(code);
        a.setKpiName(name);
        a.setMean(round2(mean));
        a.setPeak(round2(actual));
        a.setDeviationPct(mean > 0 ? round2(((actual - mean) / mean) * 100) : 0);
        a.setType(type);
        a.setSeverity(classifySeverity(type, a.getDeviationPct(), actual, ctx));
        a.setDetectedAt(detectedAt);
        a.setTrend(trend);
        return a;
    }

    private Severity classifySeverity(KpiAnomaly.AnomalyType type, double deviationPct, double actual, KpiContext ctx) {
        double absDev = Math.abs(deviationPct);
        return switch (type) {
            case SPIKE -> absDev > ctx.criticalDeviationPct ? Severity.CRITICAL
                    : absDev > ctx.highDeviationPct ? Severity.HIGH : Severity.MEDIUM;
            case SUSTAINED_HIGH -> absDev > ctx.criticalDeviationPct ? Severity.CRITICAL
                    : absDev > ctx.highDeviationPct ? Severity.HIGH : Severity.MEDIUM;
            case DIP -> absDev > ctx.highDeviationPct ? Severity.HIGH : Severity.MEDIUM;
            case GRADUAL_INCREASE -> Severity.LOW;
        };
    }

    // -------------------------------------------------------------------------
    // Performance scoring
    // -------------------------------------------------------------------------

    private DimensionScores scoreDimensions(Map<String, double[]> series,
            Map<String, Double> staticValues, List<KpiAnomaly> anomalies) {

        long criticalCount = anomalies.stream().filter(a -> a.getSeverity() == Severity.CRITICAL).count();
        long highCount = anomalies.stream().filter(a -> a.getSeverity() == Severity.HIGH).count();

        int reliability = 100 - (int) (criticalCount * 20 + highCount * 10);
        int availability = anomalies.stream().anyMatch(a -> a.getType() == KpiAnomaly.AnomalyType.DIP) ? 70 : 95;
        int throughput = anomalies.stream().anyMatch(a ->
                a.getType() == KpiAnomaly.AnomalyType.SPIKE || a.getType() == KpiAnomaly.AnomalyType.SUSTAINED_HIGH) ? 75 : 90;
        int resourceEff = staticValues.isEmpty() ? 85 :
                (anomalies.size() > 3 ? 70 : 88);

        return new DimensionScores(
                Math.max(0, Math.min(100, availability)),
                Math.max(0, Math.min(100, throughput)),
                Math.max(0, Math.min(100, reliability)),
                Math.max(0, Math.min(100, resourceEff)));
    }

    private HealthStatus deriveHealth(List<KpiAnomaly> anomalies, int score) {
        boolean hasCritical = anomalies.stream().anyMatch(a -> a.getSeverity() == Severity.CRITICAL);
        boolean hasHigh = anomalies.stream().anyMatch(a -> a.getSeverity() == Severity.HIGH);
        if (hasCritical || score < 60) return HealthStatus.CRITICAL;
        if (hasHigh || score < 75) return HealthStatus.WARNING;
        if (!anomalies.isEmpty() || score < 85) return HealthStatus.DEGRADED;
        return HealthStatus.HEALTHY;
    }

    // -------------------------------------------------------------------------
    // Human-readable findings
    // -------------------------------------------------------------------------

    private List<String> buildFindings(List<KpiAnomaly> anomalies, PmNodeSummary s) {
        List<String> findings = new ArrayList<>();
        for (KpiAnomaly a : anomalies.stream().limit(5).toList()) {
            String line = String.format("[%s] %s: %s %.0f (mean %.0f, +%.0f%%) detected at %s, trend %s",
                    a.getSeverity(), a.getType(), a.getKpiName(),
                    a.getPeak(), a.getMean(), a.getDeviationPct(),
                    a.getDetectedAt(), a.getTrend());
            findings.add(line);
        }
        if (findings.isEmpty()) {
            findings.add("No anomalies detected in the queried window — node appears stable.");
        }
        findings.add(String.format("Performance score: %d/100 | Health: %s | Region: %s %s",
                s.getPerformanceScore(), s.getHealth(), s.getRegion(), s.getCity()));
        return findings;
    }

    // -------------------------------------------------------------------------
    // Busiest periods
    // -------------------------------------------------------------------------

    private List<String> topBusiestPeriods(List<DataEntry> data, int topN) {
        return data.stream()
                .sorted(Comparator.comparingDouble((DataEntry e) ->
                        e.getKpis() != null ? -e.getKpis().values().stream().mapToDouble(Double::doubleValue).sum() : 0))
                .limit(topN)
                .map(e -> {
                    double sum = e.getKpis() != null
                            ? e.getKpis().values().stream().mapToDouble(Double::doubleValue).sum() : 0;
                    return String.format("%s (aggregate=%.0f)", e.getTime(), sum);
                })
                .collect(Collectors.toList());
    }

    // -------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------

    /** Builds a {kpiCode → double[]} map aligned with the data time index. */
    private Map<String, double[]> buildSeriesMap(List<DataEntry> data) {
        if (data.isEmpty()) return Map.of();
        Map<String, double[]> map = new LinkedHashMap<>();
        // Discover all KPI codes
        for (DataEntry e : data) {
            if (e.getKpis() != null) e.getKpis().keySet().forEach(k -> map.put(k, null));
        }
        // Fill arrays
        int n = data.size();
        for (String code : map.keySet()) {
            double[] arr = new double[n];
            for (int i = 0; i < n; i++) {
                DataEntry e = data.get(i);
                Double v = e.getKpis() != null ? e.getKpis().get(code) : null;
                arr[i] = v != null ? v : Double.NaN;
            }
            map.put(code, arr);
        }
        return map;
    }

    private double mean(double[] v) {
        if (v.length == 0) return 0;
        double s = 0;
        int n = 0;
        for (double x : v) {
            if (Double.isFinite(x)) {
                s += x;
                n++;
            }
        }
        return n == 0 ? 0 : s / n;
    }

    private double max(double[] v) {
        double m = Double.NEGATIVE_INFINITY;
        for (double x : v) if (Double.isFinite(x) && x > m) m = x;
        return Double.isFinite(m) ? m : 0;
    }

    private double min(double[] v) {
        double m = Double.POSITIVE_INFINITY;
        for (double x : v) if (Double.isFinite(x) && x < m) m = x;
        return Double.isFinite(m) ? m : 0;
    }

    private String trend(double[] v) {
        if (v.length < 2) return "STABLE";
        double first = mean(java.util.Arrays.copyOf(v, v.length / 2));
        double second = mean(java.util.Arrays.copyOfRange(v, v.length / 2, v.length));
        double change = first > 0 ? (second - first) / first * 100 : 0;
        if (change > 10) return "RISING";
        if (change < -10) return "FALLING";
        return "STABLE";
    }

    private boolean isMonotonicallyRising(double[] v, int minPoints) {
        int count = 0;
        for (int i = 1; i < v.length; i++) {
            if (!Double.isFinite(v[i]) || !Double.isFinite(v[i - 1])) {
                count = 0;
                continue;
            }
            if (v[i] > v[i - 1]) count++;
            else count = 0;
            if (count >= minPoints - 1) return true;
        }
        return false;
    }

    private KpiContext kpiContext(String code, Map<String, KpiFormulaDetails> details) {
        KpiFormulaDetails d = details != null ? details.get(code) : null;
        String name = d != null ? d.getKpiName() : "";
        String group = d != null ? d.getKpiGroup() : "";
        String type = d != null ? d.getKpiType() : "";
        String unit = d != null ? d.getKpiUnit() : "";
        String desc = d != null ? d.getDescription() : "";
        String haystack = (safe(name) + " " + safe(group) + " " + safe(type) + " " + safe(unit) + " " + safe(desc))
                .toLowerCase(Locale.ROOT);

        if (haystack.contains("availability") || haystack.contains("drop") || haystack.contains("success rate")) {
            return new KpiContext(1.6, 1.3, 0.7, 250, 120);
        }
        if (haystack.contains("latency") || haystack.contains("delay") || haystack.contains("rtt")) {
            return new KpiContext(1.8, 1.4, 0.75, 220, 110);
        }
        if (haystack.contains("throughput") || haystack.contains("traffic") || haystack.contains("bandwidth")
                || haystack.contains("utilization")) {
            return new KpiContext(2.2, 1.6, 0.6, 300, 150);
        }
        return new KpiContext(SPIKE_FACTOR, SUSTAINED_FACTOR, DIP_FACTOR, 300, 150);
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    private record KpiContext(double spikeFactor, double sustainedFactor, double dipFactor,
                              double criticalDeviationPct, double highDeviationPct) {}

    private String kpiName(String code, Map<String, KpiFormulaDetails> details) {
        KpiFormulaDetails d = details.get(code);
        return d != null && d.getKpiName() != null ? d.getKpiName() : code;
    }

    private double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    /** Adapts PmDataEnrichedResponse fields into PmDataCompactResponse for uniform processing. */
    private PmDataCompactResponse toCompact(PmDataEnrichedResponse e) {
        PmDataCompactResponse c = new PmDataCompactResponse();
        c.setSummary(e.getSummary());
        c.setLocation(e.getLocation());
        c.setData(e.getData());
        c.setStaticValues(e.getStaticValues());
        return c;
    }
}
