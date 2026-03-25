package org.springaicommunity.nova.pm.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springaicommunity.nova.pm.analytics.KpiSlaBand;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse.DataEntry;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse.Summary;
import org.springaicommunity.nova.pm.dto.PmDataEnrichedResponse;

class PmSlaThresholdCalibrationServiceTest {

    private final PmSlaThresholdCalibrationService svc = new PmSlaThresholdCalibrationService();

    @Test
    void deriveBands_minMaxAcrossNodes_mergedYamlSchema() {
        PmDataEnrichedResponse a = node("n1", List.of(
                entry(Map.of("K1", 10.0, "K2", 100.0))),
                Map.of("S", 5.0));
        PmDataEnrichedResponse b = node("n2", List.of(
                entry(Map.of("K1", 20.0, "K2", 200.0))),
                Map.of());
        Map<String, KpiSlaBand> bands = svc.deriveBands(List.of(a, b));
        assertThat(bands.get("K1").getCritHigh()).isCloseTo(20 * 1.15, offset(1e-9));
        assertThat(bands.get("K2").getWarnHigh()).isCloseTo(200 * 1.06, offset(1e-6));
        assertThat(bands.get("S").getWarnLow()).isCloseTo(5 * 0.92, offset(1e-9));
        String yaml = svc.formatYaml(bands);
        assertThat(yaml).contains("pm:").contains("sla-thresholds:").contains("\"K1\":");
    }

    private static PmDataEnrichedResponse node(String name, List<DataEntry> data, Map<String, Double> statics) {
        PmDataEnrichedResponse e = new PmDataEnrichedResponse();
        Summary s = new Summary();
        s.setNode(name);
        e.setSummary(s);
        e.setData(data);
        e.setStaticValues(statics.isEmpty() ? null : statics);
        return e;
    }

    private static DataEntry entry(Map<String, Double> kpis) {
        return new DataEntry("t0", new LinkedHashMap<>(kpis));
    }
}
