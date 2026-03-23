package org.springaicommunity.nova.pm.dto;

import java.time.Instant;
import java.util.Map;

/**
 * A single timestamped PM data point returned in the API response.
 *
 * <p>kpis — KPI codes mapped to their parsed numeric values.
 * meta  — entity metadata from the Cassandra metajson column, giving AI agents
 *         the operational context needed for analysis: entity name, geography
 *         hierarchy (L1/L2/L3/L4), node type, hour, etc.
 */
public class PmDataPoint {

    private Instant timestamp;

    /** KPI code → numeric value. Null values indicate unparseable or absent KPIs. */
    private Map<String, Double> kpis;

    /**
     * Entity metadata from the {@code metajson} column.
     * Useful fields for AI agents include:
     * <ul>
     *   <li>ENTITY_NAME — human-readable node name</li>
     *   <li>NODE_TYPE / ENTITY_TYPE — e.g. ROUTER</li>
     *   <li>L1 / L2 / L3 / L4 — geography hierarchy</li>
     *   <li>HR — hour of the reading</li>
     *   <li>DT — display date</li>
     * </ul>
     */
    private Map<String, String> meta;

    /** Equipment / network type. Example: "Router". */
    private String networkType;

    public PmDataPoint() {
    }

    public PmDataPoint(Instant timestamp, Map<String, Double> kpis,
            Map<String, String> meta, String networkType) {
        this.timestamp = timestamp;
        this.kpis = kpis;
        this.meta = meta;
        this.networkType = networkType;
    }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public Map<String, Double> getKpis() { return kpis; }
    public void setKpis(Map<String, Double> kpis) { this.kpis = kpis; }

    public Map<String, String> getMeta() { return meta; }
    public void setMeta(Map<String, String> meta) { this.meta = meta; }

    public String getNetworkType() { return networkType; }
    public void setNetworkType(String networkType) { this.networkType = networkType; }

}
