package org.springaicommunity.nova.pm.analytics;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springaicommunity.nova.pm.analytics.PmNodeSummary.DimensionScores;
import org.springaicommunity.nova.pm.analytics.PmNodeSummary.HealthStatus;
import org.springaicommunity.nova.pm.analytics.PmNodeSummary.KpiAnomaly;
import org.springaicommunity.nova.pm.analytics.PmNodeSummary.KpiAnomaly.Severity;
import org.springaicommunity.nova.pm.analytics.PmNodeSummary.KpiThresholdBreach;
import org.springaicommunity.nova.pm.analytics.PmNodeSummary.KpiWindowStat;
import org.springaicommunity.nova.pm.dto.KpiFormulaDetails;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse.DataEntry;
import org.springaicommunity.nova.pm.dto.PmDataEnrichedResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.Arrays;

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
 *   <li>Mean, peak, min per KPI across the time window (missing samples excluded, not treated as zero)</li>
 *   <li>Trend direction (RISING / FALLING / STABLE) on finite samples only</li>
 *   <li>Spike detection (value &gt; 2× mean at worst offending point)</li>
 *   <li>Sustained high (value &gt; 1.5× mean for ≥ 3 consecutive present samples)</li>
 *   <li>Gradual increase (monotonically rising ≥ 4 consecutive present samples)</li>
 *   <li>Dip detection (value &lt; 0.5× mean at worst offending point)</li>
 *   <li>Static KPIs (hoisted into {@code staticValues} by the compact mapper) merged as flat series for analysis</li>
 *   <li>Top-3 busiest time points by per-KPI load relative to each KPI's window maximum (unit-agnostic)</li>
 *   <li>5-dimension performance scoring and composite score</li>
 *   <li>Health status classification</li>
 *   <li>SLA threshold breach vs {@code pm/kpi-sla-thresholds.yaml} (worst timestep per KPI)</li>
 * </ul>
 *
 * <p><b>Note on {@code staticValues}:</b> In {@link org.springaicommunity.nova.pm.mapper.PmCompactMapper},
 * these are KPIs whose value is identical at every timestamp (compression), not SLA bands.
 * SLA bands are loaded from {@code classpath:pm/kpi-sla-thresholds.yaml} via {@link PmKpiSlaThresholdRegistry}.
 */
@Component
public class PmAnalyticsEngine {

    private static final Logger log = LoggerFactory.getLogger(PmAnalyticsEngine.class);

    private final PmKpiSlaThresholdRegistry slaRegistry;

    @Autowired
    public PmAnalyticsEngine(PmKpiSlaThresholdRegistry slaRegistry) {
        this.slaRegistry = slaRegistry != null ? slaRegistry : PmKpiSlaThresholdRegistry.empty();
    }

    // Spike: single point > SPIKE_FACTOR × mean
    private static final double SPIKE_FACTOR = 2.0;
    // Sustained: value > SUSTAINED_FACTOR × mean for >= SUSTAINED_MIN_POINTS consecutive points
    private static final double SUSTAINED_FACTOR = 1.5;
    private static final int SUSTAINED_MIN_POINTS = 3;
    // Dip: value < DIP_FACTOR × mean
    private static final double DIP_FACTOR = 0.5;
    // Gradual increase: monotonically rising >= this many present points
    private static final int GRADUAL_MIN_POINTS = 4;
    /** Minimum finite samples required before ratio-based rules use the window mean. */
    private static final int MIN_SAMPLES_FOR_RATIO_RULES = 2;

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
        Map<String, Double> staticValues =
                compact.getStaticValues() != null ? compact.getStaticValues() : Map.of();
        Map<String, double[]> seriesMap = buildSeriesMap(data, staticValues);

        // ── Anomaly detection ─────────────────────────────────────────────────
        List<KpiAnomaly> anomalies = new ArrayList<>();
        List<KpiThresholdBreach> thresholdBreaches = new ArrayList<>();
        List<KpiWindowStat> windowStats = new ArrayList<>();
        for (Map.Entry<String, double[]> e : seriesMap.entrySet()) {
            String code = e.getKey();
            double[] values = e.getValue();
            String kpiName = kpiName(code, kpiDetails);
            String trend = trend(values);
            windowStats.add(windowStat(code, kpiName, values, trend));
            detectAnomalies(code, kpiName, values, data, anomalies, trend);
            slaRegistry.bandForKpi(code).ifPresent(band ->
                    detectThresholdBreaches(code, kpiName, values, data, trend, band, anomalies, thresholdBreaches));
        }
        // Sort: CRITICAL first, then by deviation descending
        anomalies.sort(Comparator
                .comparingInt((KpiAnomaly a) -> -a.getSeverity().ordinal())
                .thenComparingDouble(a -> -Math.abs(a.getDeviationPct())));
        summary.setAnomalies(anomalies);
        summary.setThresholdBreaches(thresholdBreaches);
        summary.setKpiWindowStats(windowStats);

        // ── Busiest periods ───────────────────────────────────────────────────
        summary.setBusiestPeriods(topBusiestPeriods(data, 3));

        // ── Performance score ─────────────────────────────────────────────────
        DimensionScores dims = scoreDimensions(staticValues, anomalies);
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

    /**
     * Detects anomalies for one KPI. Missing samples ({@code NaN}) break runs and are excluded from the mean.
     * Up to one finding per rule class (spike, sustained, dip, gradual) may be emitted for the same KPI.
     */
    private void detectAnomalies(String code, String kpiName, double[] values,
            List<DataEntry> data, List<KpiAnomaly> out, String trend) {

        int nFinite = countFinite(values);
        if (nFinite == 0) {
            return;
        }

        double mean = meanValid(values);
        if (Double.isNaN(mean)) {
            return;
        }

        double peak = maxValid(values);

        boolean canUseRatio = nFinite >= MIN_SAMPLES_FOR_RATIO_RULES && mean > 0;

        // Spike — worst offending timestep (max value/mean)
        if (canUseRatio) {
            int spikeIdx = -1;
            double bestRatio = 0;
            for (int i = 0; i < values.length; i++) {
                if (!isFinite(values[i])) {
                    continue;
                }
                if (values[i] > SPIKE_FACTOR * mean) {
                    double ratio = values[i] / mean;
                    if (ratio > bestRatio) {
                        bestRatio = ratio;
                        spikeIdx = i;
                    }
                }
            }
            if (spikeIdx >= 0) {
                out.add(anomaly(code, kpiName, mean, values[spikeIdx], trend,
                        KpiAnomaly.AnomalyType.SPIKE, data.get(spikeIdx).getTime()));
            }
        }

        // Sustained high — first qualifying run; NaN breaks the run
        if (canUseRatio) {
            int run = 0;
            int runStart = -1;
            for (int i = 0; i < values.length; i++) {
                if (!isFinite(values[i])) {
                    run = 0;
                    continue;
                }
                if (values[i] > SUSTAINED_FACTOR * mean) {
                    if (run == 0) {
                        runStart = i;
                    }
                    run++;
                    if (run >= SUSTAINED_MIN_POINTS) {
                        out.add(anomaly(code, kpiName, mean, values[runStart], trend,
                                KpiAnomaly.AnomalyType.SUSTAINED_HIGH, data.get(runStart).getTime()));
                        break;
                    }
                } else {
                    run = 0;
                }
            }
        }

        // Dip — worst offending timestep (min value/mean)
        if (canUseRatio) {
            int dipIdx = -1;
            double worstRatio = Double.POSITIVE_INFINITY;
            for (int i = 0; i < values.length; i++) {
                if (!isFinite(values[i])) {
                    continue;
                }
                if (values[i] < DIP_FACTOR * mean) {
                    double ratio = values[i] / mean;
                    if (ratio < worstRatio) {
                        worstRatio = ratio;
                        dipIdx = i;
                    }
                }
            }
            if (dipIdx >= 0) {
                out.add(anomaly(code, kpiName, mean, values[dipIdx], trend,
                        KpiAnomaly.AnomalyType.DIP, data.get(dipIdx).getTime()));
            }
        }

        // Gradual increase — strictly rising along the timeline; missing samples break the run
        if (values.length >= GRADUAL_MIN_POINTS && isMonotonicallyRisingAlongTimeline(values, GRADUAL_MIN_POINTS)) {
            out.add(anomaly(code, kpiName, mean, peak, trend,
                    KpiAnomaly.AnomalyType.GRADUAL_INCREASE, data.get(0).getTime()));
        }
    }

    /**
     * One worst SLA breach per KPI (highest severity, then largest margin past the band).
     */
    private void detectThresholdBreaches(String code, String kpiName, double[] values, List<DataEntry> data,
            String trend, KpiSlaBand band, List<KpiAnomaly> out, List<KpiThresholdBreach> breachOut) {
        ThresholdPick best = null;
        for (int i = 0; i < values.length; i++) {
            if (!isFinite(values[i])) {
                continue;
            }
            ThresholdPick p = evalThresholdAt(values[i], band, i);
            if (p != null && (best == null || p.isWorseThan(best))) {
                best = p;
            }
        }
        if (best != null) {
            out.add(thresholdAnomaly(code, kpiName, best.actual(), best.limit(), best.highSide(), best.severity(),
                    data.get(best.idx()).getTime(), trend));
            breachOut.add(thresholdBreach(code, kpiName, best, data.get(best.idx()).getTime(), trend));
        }
    }

    private ThresholdPick evalThresholdAt(double v, KpiSlaBand band, int idx) {
        if (band.getCritHigh() != null && v > band.getCritHigh()) {
            return new ThresholdPick(idx, v, band.getCritHigh(), true, Severity.CRITICAL, "CRIT_HIGH");
        }
        if (band.getCritLow() != null && v < band.getCritLow()) {
            return new ThresholdPick(idx, v, band.getCritLow(), false, Severity.CRITICAL, "CRIT_LOW");
        }
        if (band.getWarnHigh() != null && v > band.getWarnHigh()) {
            return new ThresholdPick(idx, v, band.getWarnHigh(), true, Severity.HIGH, "WARN_HIGH");
        }
        if (band.getWarnLow() != null && v < band.getWarnLow()) {
            return new ThresholdPick(idx, v, band.getWarnLow(), false, Severity.HIGH, "WARN_LOW");
        }
        return null;
    }

    private record ThresholdPick(int idx, double actual, double limit, boolean highSide, Severity severity, String thresholdType) {
        double excessPastLimit() {
            return highSide ? actual - limit : limit - actual;
        }

        boolean isWorseThan(ThresholdPick o) {
            int c = Integer.compare(severity.ordinal(), o.severity.ordinal());
            if (c != 0) {
                return c > 0;
            }
            return Double.compare(excessPastLimit(), o.excessPastLimit()) > 0;
        }
    }

    private KpiAnomaly thresholdAnomaly(String code, String name, double actual, double limit, boolean highSide,
            Severity severity, String detectedAt, String trend) {
        KpiAnomaly a = new KpiAnomaly();
        a.setKpiCode(code);
        a.setKpiName(name);
        a.setMean(round2(limit));
        a.setPeak(round2(actual));
        if (limit != 0 && Double.isFinite(limit)) {
            a.setDeviationPct(round2(((actual - limit) / Math.abs(limit)) * 100));
        } else {
            a.setDeviationPct(actual > limit ? 100 : (actual < limit ? -100 : 0));
        }
        a.setType(highSide ? KpiAnomaly.AnomalyType.THRESHOLD_HIGH : KpiAnomaly.AnomalyType.THRESHOLD_LOW);
        a.setSeverity(severity);
        a.setDetectedAt(detectedAt);
        a.setTrend(trend);
        return a;
    }

    private KpiThresholdBreach thresholdBreach(String code, String name, ThresholdPick pick, String detectedAt, String trend) {
        KpiThresholdBreach b = new KpiThresholdBreach();
        b.setKpiCode(code);
        b.setKpiName(name);
        b.setActualValue(round2(pick.actual()));
        b.setThresholdValue(round2(pick.limit()));
        b.setThresholdType(pick.thresholdType());
        if (pick.limit() != 0 && Double.isFinite(pick.limit())) {
            b.setDeviationPct(round2(((pick.actual() - pick.limit()) / Math.abs(pick.limit())) * 100));
        } else {
            b.setDeviationPct(pick.actual() > pick.limit() ? 100 : (pick.actual() < pick.limit() ? -100 : 0));
        }
        b.setDetectedAt(detectedAt);
        b.setTrend(trend);
        b.setSeverity(pick.severity());
        return b;
    }

    private static KpiWindowStat windowStat(String code, String name, double[] values, String trend) {
        KpiWindowStat s = new KpiWindowStat();
        s.setKpiCode(code);
        s.setKpiName(name);
        int samples = countFinite(values);
        s.setSamples(samples);
        s.setTrend(trend);
        if (samples == 0) {
            s.setLatest(Double.NaN);
            s.setMean(Double.NaN);
            s.setMin(Double.NaN);
            s.setMax(Double.NaN);
            s.setP95(Double.NaN);
            return s;
        }
        s.setLatest(round2(latestFinite(values)));
        s.setMean(round2(meanValid(values)));
        s.setMin(round2(minValid(values)));
        s.setMax(round2(maxValid(values)));
        s.setP95(round2(p95Valid(values)));
        return s;
    }

    private static double latestFinite(double[] values) {
        for (int i = values.length - 1; i >= 0; i--) {
            if (isFinite(values[i])) return values[i];
        }
        return Double.NaN;
    }

    private static double p95Valid(double[] values) {
        double[] finite = Arrays.stream(values).filter(PmAnalyticsEngine::isFinite).toArray();
        if (finite.length == 0) return Double.NaN;
        Arrays.sort(finite);
        int idx = (int) Math.ceil(0.95 * finite.length) - 1;
        idx = Math.max(0, Math.min(finite.length - 1, idx));
        return finite[idx];
    }

    private KpiAnomaly anomaly(String code, String name, double mean, double actual,
            String trend, KpiAnomaly.AnomalyType type, String detectedAt) {
        KpiAnomaly a = new KpiAnomaly();
        a.setKpiCode(code);
        a.setKpiName(name);
        a.setMean(round2(mean));
        a.setPeak(round2(actual));
        a.setDeviationPct(mean > 0 ? round2(((actual - mean) / mean) * 100) : 0);
        a.setType(type);
        a.setSeverity(classifySeverity(type, a.getDeviationPct()));
        a.setDetectedAt(detectedAt);
        a.setTrend(trend);
        return a;
    }

    private Severity classifySeverity(KpiAnomaly.AnomalyType type, double deviationPct) {
        double ad = Math.abs(deviationPct);
        return switch (type) {
            case SPIKE -> ad > 500 ? Severity.CRITICAL : ad > 200 ? Severity.HIGH : Severity.MEDIUM;
            case SUSTAINED_HIGH -> ad > 500 ? Severity.CRITICAL : ad > 200 ? Severity.HIGH : Severity.MEDIUM;
            case DIP -> ad > 80 ? Severity.HIGH : Severity.MEDIUM;
            case GRADUAL_INCREASE -> Severity.LOW;
            case THRESHOLD_HIGH, THRESHOLD_LOW -> Severity.MEDIUM;
        };
    }

    // -------------------------------------------------------------------------
    // Performance scoring
    // -------------------------------------------------------------------------

    private DimensionScores scoreDimensions(Map<String, Double> staticValues, List<KpiAnomaly> anomalies) {

        long criticalCount = anomalies.stream().filter(a -> a.getSeverity() == Severity.CRITICAL).count();
        long highCount = anomalies.stream().filter(a -> a.getSeverity() == Severity.HIGH).count();

        int reliability = 100 - (int) (criticalCount * 20 + highCount * 10);
        int availability = anomalies.stream().anyMatch(a ->
                a.getType() == KpiAnomaly.AnomalyType.DIP
                        || a.getType() == KpiAnomaly.AnomalyType.THRESHOLD_LOW) ? 70 : 95;
        int throughput = anomalies.stream().anyMatch(a ->
                a.getType() == KpiAnomaly.AnomalyType.SPIKE
                        || a.getType() == KpiAnomaly.AnomalyType.SUSTAINED_HIGH
                        || a.getType() == KpiAnomaly.AnomalyType.THRESHOLD_HIGH) ? 75 : 90;
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
        if (hasCritical || score < 60) {
            return HealthStatus.CRITICAL;
        }
        if (hasHigh || score < 75) {
            return HealthStatus.WARNING;
        }
        if (!anomalies.isEmpty() || score < 85) {
            return HealthStatus.DEGRADED;
        }
        return HealthStatus.HEALTHY;
    }

    // -------------------------------------------------------------------------
    // Human-readable findings
    // -------------------------------------------------------------------------

    private List<String> buildFindings(List<KpiAnomaly> anomalies, PmNodeSummary s) {
        List<String> findings = new ArrayList<>();
        for (KpiAnomaly a : anomalies.stream().limit(5).toList()) {
            String devStr = formatDeviationPct(a.getDeviationPct());
            boolean sla = a.getType() == KpiAnomaly.AnomalyType.THRESHOLD_HIGH
                    || a.getType() == KpiAnomaly.AnomalyType.THRESHOLD_LOW;
            String refLabel = sla ? "SLA limit" : "mean";
            String line = String.format("[%s] %s: %s %.4g (%s %.4g, %s) at %s, trend %s",
                    a.getSeverity(), a.getType(), a.getKpiName(),
                    a.getPeak(), refLabel, a.getMean(), devStr,
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

    private static String formatDeviationPct(double deviationPct) {
        if (deviationPct > 0) {
            return String.format("+%.0f%% vs mean", deviationPct);
        }
        if (deviationPct < 0) {
            return String.format("%.0f%% vs mean", deviationPct);
        }
        return "0% vs mean";
    }

    // -------------------------------------------------------------------------
    // Busiest periods
    // -------------------------------------------------------------------------

    /**
     * Ranks timesteps by sum of (value / max_k) over varying KPIs — avoids adding incompatible units.
     */
    private List<String> topBusiestPeriods(List<DataEntry> data, int topN) {
        Map<String, Double> maxByKpi = maxKpiPerCodeAcrossWindow(data);
        return data.stream()
                .sorted(Comparator.comparingDouble((DataEntry e) -> -normalizedPeriodLoad(e, maxByKpi)))
                .limit(topN)
                .map(e -> {
                    double score = normalizedPeriodLoad(e, maxByKpi);
                    return String.format("%s (normalized_load=%.2f)", e.getTime(), score);
                })
                .toList();
    }

    private static Map<String, Double> maxKpiPerCodeAcrossWindow(List<DataEntry> data) {
        Map<String, Double> max = new HashMap<>();
        for (DataEntry e : data) {
            if (e.getKpis() == null) {
                continue;
            }
            for (Map.Entry<String, Double> en : e.getKpis().entrySet()) {
                Double v = en.getValue();
                if (v == null || !isFinite(v)) {
                    continue;
                }
                max.merge(en.getKey(), v, Math::max);
            }
        }
        return max;
    }

    /**
     * Sum of value/max for each KPI at this timestep (KPIs with max &lt;= 0 are skipped).
     */
    private static double normalizedPeriodLoad(DataEntry e, Map<String, Double> maxByKpi) {
        if (e.getKpis() == null || e.getKpis().isEmpty()) {
            return 0;
        }
        double sum = 0;
        int parts = 0;
        for (Map.Entry<String, Double> en : e.getKpis().entrySet()) {
            Double v = en.getValue();
            if (v == null || !isFinite(v)) {
                continue;
            }
            double mx = maxByKpi.getOrDefault(en.getKey(), 0.0);
            if (mx <= 0) {
                continue;
            }
            sum += v / mx;
            parts++;
        }
        return parts > 0 ? sum : 0;
    }

    // -------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------

    /**
     * Builds a {kpiCode → double[]} aligned with {@code data} indices. Missing values are {@link Double#NaN}.
     * KPIs present only in {@code staticValues} get a constant series (compact mapper hoists them out of entries).
     */
    private Map<String, double[]> buildSeriesMap(List<DataEntry> data, Map<String, Double> staticValues) {
        if (data.isEmpty() && staticValues.isEmpty()) {
            return Map.of();
        }
        Map<String, double[]> map = new LinkedHashMap<>();
        for (DataEntry e : data) {
            if (e.getKpis() != null) {
                e.getKpis().keySet().forEach(k -> map.put(k, null));
            }
        }
        staticValues.keySet().forEach(k -> map.putIfAbsent(k, null));

        int n = data.size();
        for (String code : map.keySet()) {
            double[] arr = new double[n];
            Double constant = staticValues.get(code);
            for (int i = 0; i < n; i++) {
                DataEntry e = data.get(i);
                Double v = e.getKpis() != null ? e.getKpis().get(code) : null;
                if (v != null) {
                    arr[i] = v;
                } else if (constant != null) {
                    arr[i] = constant;
                } else {
                    arr[i] = Double.NaN;
                }
            }
            map.put(code, arr);
        }
        return map;
    }

    private static boolean isFinite(double x) {
        return !Double.isNaN(x) && Double.isFinite(x);
    }

    private static int countFinite(double[] v) {
        int c = 0;
        for (double x : v) {
            if (isFinite(x)) {
                c++;
            }
        }
        return c;
    }

    private static double meanValid(double[] v) {
        double s = 0;
        int n = 0;
        for (double x : v) {
            if (isFinite(x)) {
                s += x;
                n++;
            }
        }
        return n > 0 ? s / n : Double.NaN;
    }

    private static double maxValid(double[] v) {
        double m = Double.NEGATIVE_INFINITY;
        boolean any = false;
        for (double x : v) {
            if (isFinite(x)) {
                any = true;
                if (x > m) {
                    m = x;
                }
            }
        }
        return any ? m : Double.NaN;
    }

    private static double minValid(double[] v) {
        double m = Double.POSITIVE_INFINITY;
        boolean any = false;
        for (double x : v) {
            if (isFinite(x)) {
                any = true;
                if (x < m) {
                    m = x;
                }
            }
        }
        return any ? m : Double.NaN;
    }

    private String trend(double[] v) {
        double[] valid = finiteValues(v);
        if (valid.length < 2) {
            return "STABLE";
        }
        int mid = valid.length / 2;
        if (mid < 1) {
            return "STABLE";
        }
        double first = meanOfArray(valid, 0, mid);
        double second = meanOfArray(valid, mid, valid.length);
        double change;
        if (first > 0) {
            change = (second - first) / first * 100;
        } else {
            change = 0;
        }
        if (change > 10) {
            return "RISING";
        }
        if (change < -10) {
            return "FALLING";
        }
        return "STABLE";
    }

    private static double[] finiteValues(double[] v) {
        int n = countFinite(v);
        if (n == 0) {
            return new double[0];
        }
        if (n == v.length) {
            return v.clone();
        }
        double[] out = new double[n];
        int j = 0;
        for (double x : v) {
            if (isFinite(x)) {
                out[j++] = x;
            }
        }
        return out;
    }

    /** Mean of {@code arr[start..end)} (no NaNs expected). */
    private static double meanOfArray(double[] arr, int start, int end) {
        double s = 0;
        int n = end - start;
        for (int i = start; i < end; i++) {
            s += arr[i];
        }
        return n > 0 ? s / n : 0;
    }

    /**
     * True if there is a run of {@code minPoints} timestamps with strictly increasing values;
     * {@link Double#NaN} at an index breaks the run (unknown sample).
     */
    private static boolean isMonotonicallyRisingAlongTimeline(double[] v, int minPoints) {
        int count = 0;
        for (int i = 1; i < v.length; i++) {
            if (!isFinite(v[i]) || !isFinite(v[i - 1])) {
                count = 0;
                continue;
            }
            if (v[i] > v[i - 1]) {
                count++;
                if (count >= minPoints - 1) {
                    return true;
                }
            } else {
                count = 0;
            }
        }
        return false;
    }

    private String kpiName(String code, Map<String, KpiFormulaDetails> details) {
        KpiFormulaDetails d = details.get(code);
        return d != null && d.getKpiName() != null ? d.getKpiName() : code;
    }

    private static double round2(double v) {
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
