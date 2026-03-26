package org.springaicommunity.nova.pm.analytics;

import java.util.List;

/**
 * Pre-computed analytics summary for a single PM node.
 *
 * <p>This is what the {@link PmAnalyticsEngine} produces from raw
 * {@link org.springaicommunity.nova.pm.dto.PmDataCompactResponse} data.
 * All heavy number-crunching (mean, trend, statistical anomaly detection)
 * happens in Java. The LLM only receives this compact summary — never raw
 * time-series.
 *
 * <p>Token budget: ~100–300 tokens per node vs ~5,000 for raw PM JSON.
 * At 1 lakh nodes, Top-50 offenders = ~15,000 tokens total vs 250M tokens raw.
 */
public class PmNodeSummary {

    /** Node identity — copied from PM summary. */
    private String node;
    private String vendor;
    private String domain;
    private String technology;
    private String granularity;
    private String period;
    private int totalPoints;

    /** Location context. */
    private String region;
    private String state;
    private String city;

    /** Overall health derived by the engine. */
    private HealthStatus health;

    /** 0-100 composite performance score. */
    private int performanceScore;

    /** Per-dimension scores that make up performanceScore. */
    private DimensionScores dimensionScores;

    /** Detected anomalies — only KPIs that breached a threshold. */
    private List<KpiAnomaly> anomalies;

    /**
     * Explicit SLA threshold breaches with both actual and threshold values.
     * This exists so the LLM can render the "KPI Threshold Breaches" section
     * without inferring thresholds from other fields.
     */
    private List<KpiThresholdBreach> thresholdBreaches;

    /**
     * Window statistics per KPI (latest/mean/min/max/p95) to support degradation
     * and benchmarking-style comparisons when external baselines are not available.
     */
    private List<KpiWindowStat> kpiWindowStats;

    /** Top-3 busiest time points. */
    private List<String> busiestPeriods;

    /** Brief natural-language findings (3-5 bullets). */
    private List<String> findings;

    // -------------------------------------------------------------------------

    public enum HealthStatus { HEALTHY, DEGRADED, WARNING, CRITICAL }

    public static class DimensionScores {
        private int availability;
        private int throughput;
        private int reliability;
        private int resourceEfficiency;

        public DimensionScores() {}
        public DimensionScores(int availability, int throughput, int reliability, int resourceEfficiency) {
            this.availability = availability;
            this.throughput = throughput;
            this.reliability = reliability;
            this.resourceEfficiency = resourceEfficiency;
        }

        public int getAvailability() { return availability; }
        public void setAvailability(int availability) { this.availability = availability; }
        public int getThroughput() { return throughput; }
        public void setThroughput(int throughput) { this.throughput = throughput; }
        public int getReliability() { return reliability; }
        public void setReliability(int reliability) { this.reliability = reliability; }
        public int getResourceEfficiency() { return resourceEfficiency; }
        public void setResourceEfficiency(int resourceEfficiency) { this.resourceEfficiency = resourceEfficiency; }
    }

    public static class KpiAnomaly {
        private String kpiCode;
        private String kpiName;
        private double mean;
        private double peak;
        private double deviationPct;
        private AnomalyType type;
        private Severity severity;
        private String detectedAt;
        private String trend;

        public KpiAnomaly() {}

        public enum AnomalyType {
            SPIKE,
            SUSTAINED_HIGH,
            GRADUAL_INCREASE,
            DIP,
            /** Value above YAML {@code warn-high} / {@code crit-high}. */
            THRESHOLD_HIGH,
            /** Value below YAML {@code warn-low} / {@code crit-low}. */
            THRESHOLD_LOW
        }
        public enum Severity { LOW, MEDIUM, HIGH, CRITICAL }

        public String getKpiCode() { return kpiCode; }
        public void setKpiCode(String kpiCode) { this.kpiCode = kpiCode; }
        public String getKpiName() { return kpiName; }
        public void setKpiName(String kpiName) { this.kpiName = kpiName; }
        public double getMean() { return mean; }
        public void setMean(double mean) { this.mean = mean; }
        public double getPeak() { return peak; }
        public void setPeak(double peak) { this.peak = peak; }
        public double getDeviationPct() { return deviationPct; }
        public void setDeviationPct(double deviationPct) { this.deviationPct = deviationPct; }
        public AnomalyType getType() { return type; }
        public void setType(AnomalyType type) { this.type = type; }
        public Severity getSeverity() { return severity; }
        public void setSeverity(Severity severity) { this.severity = severity; }
        public String getDetectedAt() { return detectedAt; }
        public void setDetectedAt(String detectedAt) { this.detectedAt = detectedAt; }
        public String getTrend() { return trend; }
        public void setTrend(String trend) { this.trend = trend; }
    }

    public static class KpiThresholdBreach {
        private String kpiCode;
        private String kpiName;
        private double actualValue;
        private double thresholdValue;
        private String thresholdType; // e.g. CRIT_HIGH, WARN_LOW
        private double deviationPct;
        private String detectedAt;
        private String trend;
        private KpiAnomaly.Severity severity;

        public String getKpiCode() { return kpiCode; }
        public void setKpiCode(String kpiCode) { this.kpiCode = kpiCode; }
        public String getKpiName() { return kpiName; }
        public void setKpiName(String kpiName) { this.kpiName = kpiName; }
        public double getActualValue() { return actualValue; }
        public void setActualValue(double actualValue) { this.actualValue = actualValue; }
        public double getThresholdValue() { return thresholdValue; }
        public void setThresholdValue(double thresholdValue) { this.thresholdValue = thresholdValue; }
        public String getThresholdType() { return thresholdType; }
        public void setThresholdType(String thresholdType) { this.thresholdType = thresholdType; }
        public double getDeviationPct() { return deviationPct; }
        public void setDeviationPct(double deviationPct) { this.deviationPct = deviationPct; }
        public String getDetectedAt() { return detectedAt; }
        public void setDetectedAt(String detectedAt) { this.detectedAt = detectedAt; }
        public String getTrend() { return trend; }
        public void setTrend(String trend) { this.trend = trend; }
        public KpiAnomaly.Severity getSeverity() { return severity; }
        public void setSeverity(KpiAnomaly.Severity severity) { this.severity = severity; }
    }

    public static class KpiWindowStat {
        private String kpiCode;
        private String kpiName;
        private int samples;
        private double latest;
        private double mean;
        private double min;
        private double max;
        private double p95;
        private String trend;

        public String getKpiCode() { return kpiCode; }
        public void setKpiCode(String kpiCode) { this.kpiCode = kpiCode; }
        public String getKpiName() { return kpiName; }
        public void setKpiName(String kpiName) { this.kpiName = kpiName; }
        public int getSamples() { return samples; }
        public void setSamples(int samples) { this.samples = samples; }
        public double getLatest() { return latest; }
        public void setLatest(double latest) { this.latest = latest; }
        public double getMean() { return mean; }
        public void setMean(double mean) { this.mean = mean; }
        public double getMin() { return min; }
        public void setMin(double min) { this.min = min; }
        public double getMax() { return max; }
        public void setMax(double max) { this.max = max; }
        public double getP95() { return p95; }
        public void setP95(double p95) { this.p95 = p95; }
        public String getTrend() { return trend; }
        public void setTrend(String trend) { this.trend = trend; }
    }

    // -------------------------------------------------------------------------
    // Getters / setters
    // -------------------------------------------------------------------------

    public String getNode() { return node; }
    public void setNode(String node) { this.node = node; }
    public String getVendor() { return vendor; }
    public void setVendor(String vendor) { this.vendor = vendor; }
    public String getDomain() { return domain; }
    public void setDomain(String domain) { this.domain = domain; }
    public String getTechnology() { return technology; }
    public void setTechnology(String technology) { this.technology = technology; }
    public String getGranularity() { return granularity; }
    public void setGranularity(String granularity) { this.granularity = granularity; }
    public String getPeriod() { return period; }
    public void setPeriod(String period) { this.period = period; }
    public int getTotalPoints() { return totalPoints; }
    public void setTotalPoints(int totalPoints) { this.totalPoints = totalPoints; }
    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }
    public String getState() { return state; }
    public void setState(String state) { this.state = state; }
    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }
    public HealthStatus getHealth() { return health; }
    public void setHealth(HealthStatus health) { this.health = health; }
    public int getPerformanceScore() { return performanceScore; }
    public void setPerformanceScore(int performanceScore) { this.performanceScore = performanceScore; }
    public DimensionScores getDimensionScores() { return dimensionScores; }
    public void setDimensionScores(DimensionScores dimensionScores) { this.dimensionScores = dimensionScores; }
    public List<KpiAnomaly> getAnomalies() { return anomalies; }
    public void setAnomalies(List<KpiAnomaly> anomalies) { this.anomalies = anomalies; }
    public List<KpiThresholdBreach> getThresholdBreaches() { return thresholdBreaches; }
    public void setThresholdBreaches(List<KpiThresholdBreach> thresholdBreaches) { this.thresholdBreaches = thresholdBreaches; }
    public List<KpiWindowStat> getKpiWindowStats() { return kpiWindowStats; }
    public void setKpiWindowStats(List<KpiWindowStat> kpiWindowStats) { this.kpiWindowStats = kpiWindowStats; }
    public List<String> getBusiestPeriods() { return busiestPeriods; }
    public void setBusiestPeriods(List<String> busiestPeriods) { this.busiestPeriods = busiestPeriods; }
    public List<String> getFindings() { return findings; }
    public void setFindings(List<String> findings) { this.findings = findings; }
}
