package org.springaicommunity.nova.pm.model;

import java.util.Map;

import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

/**
 * Cassandra entity representing one PM data row.
 *
 * <p>Maps to the actual schema observed in the {@code pm} keyspace:
 * <pre>
 *   domain    | vendor | technology | datalevel | date | nodename | timestamp
 *   kpijson   map&lt;text, text&gt;  — pre-computed KPI code → string value
 *   metajson  map&lt;text, text&gt;  — entity metadata (ENTITY_NAME, L1..L4, NODE_TYPE, etc.)
 *   networktype text            — e.g. "Router"
 * </pre>
 *
 * <p>The {@code @Table} annotation references {@code combinehourlypm} as the schema
 * anchor. At runtime, all queries use the table resolved from the requested
 * {@link Granularity} via CassandraTemplate — the annotation value is not used for
 * query routing.
 *
 * <p>KPI string-to-numeric conversion is handled in
 * {@link org.springaicommunity.nova.pm.mapper.PmDataMapper}.
 */
@Table("combinehourlypm")
public class PmRecord {

    @PrimaryKey
    private PmRecordKey key;

    /** Pre-computed KPI values. Key = KPI code (e.g. "1049"), value = numeric string. */
    @Column("kpijson")
    private Map<String, String> kpiJson;

    /**
     * Entity metadata injected by the PM pipeline.
     * Contains fields such as: ENTITY_NAME, ENTITY_TYPE, NODE_TYPE,
     * L1 (region), L2, L3, L4 (geography hierarchy), DT (date-time), HR (hour), etc.
     */
    @Column("metajson")
    private Map<String, String> metaJson;

    /** Network/equipment type. Example: "Router". */
    @Column("networktype")
    private String networkType;

    public PmRecord() {
    }

    public PmRecord(PmRecordKey key, Map<String, String> kpiJson,
            Map<String, String> metaJson, String networkType) {
        this.key = key;
        this.kpiJson = kpiJson;
        this.metaJson = metaJson;
        this.networkType = networkType;
    }

    public PmRecordKey getKey() { return key; }
    public void setKey(PmRecordKey key) { this.key = key; }

    public Map<String, String> getKpiJson() { return kpiJson; }
    public void setKpiJson(Map<String, String> kpiJson) { this.kpiJson = kpiJson; }

    public Map<String, String> getMetaJson() { return metaJson; }
    public void setMetaJson(Map<String, String> metaJson) { this.metaJson = metaJson; }

    public String getNetworkType() { return networkType; }
    public void setNetworkType(String networkType) { this.networkType = networkType; }

}
