package org.springaicommunity.nova.pm.analytics;

import java.util.Objects;

/**
 * Optional high/low watermark pair loaded from {@code pm/kpi-sla-thresholds.yaml}.
 * Any bound may be null (omitted in YAML).
 */
public final class KpiSlaBand {

    private final Double warnHigh;
    private final Double critHigh;
    private final Double warnLow;
    private final Double critLow;

    public KpiSlaBand(Double warnHigh, Double critHigh, Double warnLow, Double critLow) {
        this.warnHigh = warnHigh;
        this.critHigh = critHigh;
        this.warnLow = warnLow;
        this.critLow = critLow;
    }

    public Double getWarnHigh() {
        return warnHigh;
    }

    public Double getCritHigh() {
        return critHigh;
    }

    public Double getWarnLow() {
        return warnLow;
    }

    public Double getCritLow() {
        return critLow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KpiSlaBand kpiSlaBand = (KpiSlaBand) o;
        return Objects.equals(warnHigh, kpiSlaBand.warnHigh)
                && Objects.equals(critHigh, kpiSlaBand.critHigh)
                && Objects.equals(warnLow, kpiSlaBand.warnLow)
                && Objects.equals(critLow, kpiSlaBand.critLow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(warnHigh, critHigh, warnLow, critLow);
    }
}
