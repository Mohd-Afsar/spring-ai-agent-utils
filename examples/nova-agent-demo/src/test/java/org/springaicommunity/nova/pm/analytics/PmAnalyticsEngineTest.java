package org.springaicommunity.nova.pm.analytics;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse.DataEntry;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse.Summary;
import org.springaicommunity.nova.pm.dto.PmDataEnrichedResponse;

class PmAnalyticsEngineTest {

    private final PmAnalyticsEngine engine = new PmAnalyticsEngine(PmKpiSlaThresholdRegistry.empty());

    @Test
    void missingSamplesAreNotTreatedAsZero_doesNotFabricateDip() {
        // Mean of present samples = 100; absent third point must not become 0 and trigger dip
        List<DataEntry> data = List.of(
                entry("t0", Map.of("K1", 100.0)),
                entry("t1", Map.of("K1", 100.0)),
                entry("t2", Map.of())); // K1 missing — was previously coerced to 0

        PmNodeSummary s = engine.analyze(enriched(data, Map.of()));
        assertThat(s.getAnomalies().stream().filter(a -> "K1".equals(a.getKpiCode()))).isEmpty();
    }

    @Test
    void staticKpiHoistedIntoSeries_analyzedWithoutNpe() {
        // Baseline high enough that the ramp does not classify the first point as a dip vs mean
        List<DataEntry> data = List.of(
                entry("t0", Map.of("VARY", 10.0)),
                entry("t1", Map.of("VARY", 11.0)),
                entry("t2", Map.of("VARY", 12.0)),
                entry("t3", Map.of("VARY", 13.0)));
        Map<String, Double> statics = Map.of("CONST", 42.0);

        PmNodeSummary s = engine.analyze(enriched(data, statics));
        // Constant hoisted KPI is merged into the series but should not raise statistical anomalies
        assertThat(s.getAnomalies().stream().filter(a -> "CONST".equals(a.getKpiCode()))).isEmpty();
        assertThat(s.getAnomalies().stream().anyMatch(a -> "VARY".equals(a.getKpiCode()))).isTrue();
    }

    @Test
    void busiestPeriods_useNormalizedLoad_notRawSumOfMixedUnits() {
        List<DataEntry> data = List.of(
                entry("t0", Map.of("A", 5.0, "B", 5.0)),
                entry("t1", Map.of("A", 10.0, "B", 2.0)),
                entry("t2", Map.of("A", 0.0, "B", 1.0)));
        // Max A=10, max B=5 → t0 = 0.5+1.0=1.5, t1 = 1.0+0.4=1.4, t2 = 0+0.2
        PmNodeSummary s = engine.analyze(enriched(data, Map.of()));
        assertThat(s.getBusiestPeriods().get(0)).startsWith("t0");
    }

    @Test
    void canEmitMultipleAnomalyTypesForSameKpi() {
        // Mostly 100, one dip 40, one spike 300 — mean ≈ 134.3 → dip and spike both fire
        List<DataEntry> d3 = List.of(
                entry("a", Map.of("K1", 100.0)),
                entry("b", Map.of("K1", 100.0)),
                entry("c", Map.of("K1", 100.0)),
                entry("d", Map.of("K1", 40.0)),
                entry("e", Map.of("K1", 100.0)),
                entry("f", Map.of("K1", 100.0)),
                entry("g", Map.of("K1", 300.0)));
        // mean = (600+40+300)/7 = 940/7 ≈ 134.3, dip 40 < 67 yes, spike 300 > 268 yes
        PmNodeSummary s = engine.analyze(enriched(d3, Map.of()));
        var types = s.getAnomalies().stream()
                .filter(a -> "K1".equals(a.getKpiCode()))
                .map(a -> a.getType())
                .toList();
        assertThat(types).contains(
                PmNodeSummary.KpiAnomaly.AnomalyType.DIP,
                PmNodeSummary.KpiAnomaly.AnomalyType.SPIKE);
    }

    @Test
    void slaThreshold_highBreach_emitsThresholdHigh() {
        KpiSlaBand band = new KpiSlaBand(50.0, 100.0, null, null);
        PmKpiSlaThresholdRegistry reg = new PmKpiSlaThresholdRegistry(Map.of("X", band));
        PmAnalyticsEngine eng = new PmAnalyticsEngine(reg);
        List<DataEntry> data = List.of(
                entry("t0", Map.of("X", 40.0)),
                entry("t1", Map.of("X", 150.0)));
        PmNodeSummary s = eng.analyze(enriched(data, Map.of()));
        assertThat(s.getAnomalies()).anyMatch(a ->
                "X".equals(a.getKpiCode())
                        && a.getType() == PmNodeSummary.KpiAnomaly.AnomalyType.THRESHOLD_HIGH
                        && a.getSeverity() == PmNodeSummary.KpiAnomaly.Severity.CRITICAL);
    }

    @Test
    void packagedSlaYaml_loads() throws Exception {
        org.springframework.core.io.ClassPathResource res =
                new org.springframework.core.io.ClassPathResource("pm/kpi-sla-thresholds.yaml");
        PmKpiSlaThresholdRegistry reg = new PmKpiSlaThresholdRegistry(res);
        assertThat(reg.bandForKpi("0005")).isPresent();
    }

    private static DataEntry entry(String time, Map<String, Double> kpis) {
        return new DataEntry(time, kpis.isEmpty() ? null : new LinkedHashMap<>(kpis));
    }

    private static PmDataEnrichedResponse enriched(List<DataEntry> data, Map<String, Double> statics) {
        PmDataEnrichedResponse e = new PmDataEnrichedResponse();
        Summary sum = new Summary();
        sum.setNode("n1");
        sum.setTotalPoints(data.size());
        e.setSummary(sum);
        e.setData(data);
        e.setStaticValues(statics.isEmpty() ? null : statics);
        e.setKpiDetails(Map.of());
        return e;
    }
}
