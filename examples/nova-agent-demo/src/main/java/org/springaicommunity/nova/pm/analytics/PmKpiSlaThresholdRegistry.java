package org.springaicommunity.nova.pm.analytics;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads per-KPI SLA bands from classpath {@code pm/kpi-sla-thresholds.yaml}.
 * Regenerate bands with {@code scripts/calibrate_pm_sla_yaml.py}.
 */
@Component
public class PmKpiSlaThresholdRegistry {

    private static final Logger log = LoggerFactory.getLogger(PmKpiSlaThresholdRegistry.class);

    private volatile Map<String, KpiSlaBand> kpis;

    /** Spring: load from packaged YAML. */
    @Autowired
    public PmKpiSlaThresholdRegistry(@Value("classpath:pm/kpi-sla-thresholds.yaml") Resource yamlResource) {
        this.kpis = freeze(loadFromResource(yamlResource));
    }

    /** Same-package tests / {@link #empty()}. */
    PmKpiSlaThresholdRegistry(Map<String, KpiSlaBand> kpis) {
        this.kpis = freeze(kpis);
    }

    public static PmKpiSlaThresholdRegistry empty() {
        return new PmKpiSlaThresholdRegistry(Map.of());
    }

    /**
     * Replaces in-memory SLA bands (e.g. after {@link #reloadFromYamlContent(String)} or API calibration).
     */
    public synchronized void replaceAll(Map<String, KpiSlaBand> newKpis) {
        this.kpis = freeze(newKpis);
        log.info("SLA registry replaced with {} KPI codes", newKpis.size());
    }

    /** Parses YAML text in the same schema as {@code pm/kpi-sla-thresholds.yaml} and applies it. */
    public synchronized void reloadFromYamlContent(String yamlContent) {
        if (yamlContent == null || yamlContent.isBlank()) {
            log.warn("Ignoring empty SLA YAML reload");
            return;
        }
        byte[] bytes = yamlContent.getBytes(StandardCharsets.UTF_8);
        Map<String, KpiSlaBand> parsed = parseYaml(new ByteArrayInputStream(bytes));
        replaceAll(parsed);
    }

    public Optional<KpiSlaBand> bandForKpi(String kpiCode) {
        Map<String, KpiSlaBand> m = kpis;
        return Optional.ofNullable(m.get(kpiCode));
    }

    private static Map<String, KpiSlaBand> freeze(Map<String, KpiSlaBand> m) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(m));
    }

    private static Map<String, KpiSlaBand> loadFromResource(Resource yamlResource) {
        if (yamlResource == null || !yamlResource.exists()) {
            log.warn("SLA threshold file missing — threshold breaches disabled");
            return Map.of();
        }
        try (InputStream in = yamlResource.getInputStream()) {
            return parseYaml(in);
        } catch (Exception e) {
            log.warn("Failed to load SLA thresholds — threshold breaches disabled: {}", e.getMessage());
            return Map.of();
        }
    }

    @SuppressWarnings("unchecked")
    static Map<String, KpiSlaBand> parseYaml(InputStream in) {
        Yaml yaml = new Yaml();
        Object root = yaml.load(in);
        if (!(root instanceof Map<?, ?> rootMap)) {
            return Map.of();
        }
        Object pm = rootMap.get("pm");
        if (!(pm instanceof Map<?, ?> pmMap)) {
            return Map.of();
        }
        Object sla = pmMap.get("sla-thresholds");
        if (!(sla instanceof Map<?, ?> slaMap)) {
            return Map.of();
        }
        Object kpisObj = slaMap.get("kpis");
        if (!(kpisObj instanceof Map<?, ?> kpisMap)) {
            return Map.of();
        }
        Map<String, KpiSlaBand> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : kpisMap.entrySet()) {
            String code = String.valueOf(e.getKey());
            if (!(e.getValue() instanceof Map<?, ?> bandMap)) {
                continue;
            }
            out.put(code, new KpiSlaBand(
                    d(bandMap, "warn-high"),
                    d(bandMap, "crit-high"),
                    d(bandMap, "warn-low"),
                    d(bandMap, "crit-low")));
        }
        if (!out.isEmpty()) {
            log.info("Parsed SLA bands for {} KPI codes", out.size());
        }
        return out;
    }

    private static Double d(Map<?, ?> m, String key) {
        Object v = m.get(key);
        if (v == null) {
            return null;
        }
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        return null;
    }
}
