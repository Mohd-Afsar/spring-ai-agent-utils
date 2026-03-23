package org.springaicommunity.nova.pm.dto;

import java.util.List;
import java.util.Map;

/**
 * Compact PM data response optimised for LLM token efficiency.
 *
 * <p>Key compressions vs the full {@link PmDataResponse}:
 * <ul>
 *   <li>Node / entity metadata is extracted once into {@code summary} and
 *       {@code location} — not repeated per data point.</li>
 *   <li>KPIs whose value is identical across every time point are hoisted into
 *       {@code staticValues}; only truly time-varying KPIs appear inside each
 *       {@link DataEntry}.</li>
 *   <li>Timestamps are formatted as {@code yyyy-MM-dd'T'HH:mm} (no seconds,
 *       no timezone suffix) for human readability.</li>
 * </ul>
 */
public class PmDataCompactResponse {

    private Summary summary;
    private Location location;
    private List<DataEntry> data;

    /**
     * KPI code → value for KPIs whose value did not change across the queried
     * time window. Absent from {@link DataEntry#kpis} to avoid redundancy.
     */
    private Map<String, Double> staticValues;

    public PmDataCompactResponse() {
        // no-arg constructor for JSON deserialisation
    }

    // -------------------------------------------------------------------------
    // Nested DTOs
    // -------------------------------------------------------------------------

    /** High-level context about the node and the query. */
    public static class Summary {
        /** Human-readable node label, e.g. "SC-J960-P_R1-T1-SR (172.31.31.131)". */
        private String node;
        private String type;
        private String vendor;
        private String domain;
        private String technology;
        private String granularity;
        /** ISO-8601 range label, e.g. "2025-01-19T00:00 to 2025-01-23T23:59". */
        private String period;
        private long totalPoints;

        public Summary() {
            // no-arg constructor for JSON deserialisation
        }

        public String getNode() { return node; }
        public void setNode(String node) { this.node = node; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

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

        public long getTotalPoints() { return totalPoints; }
        public void setTotalPoints(long totalPoints) { this.totalPoints = totalPoints; }
    }

    /** Geographic hierarchy extracted from Cassandra metajson (L1/L2/L3/L4). */
    public static class Location {
        /** L1 — top-level region, e.g. "SOUTH". */
        private String region;
        /** L2 — state or sub-region, e.g. "TELANGANA". */
        private String state;
        /** L3 — city or site, e.g. "HYDERABAD". */
        private String city;
        /** L4 — optional lowest hierarchy level. */
        private String site;

        public Location() {
            // no-arg constructor for JSON deserialisation
        }

        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }

        public String getState() { return state; }
        public void setState(String state) { this.state = state; }

        public String getCity() { return city; }
        public void setCity(String city) { this.city = city; }

        public String getSite() { return site; }
        public void setSite(String site) { this.site = site; }
    }

    /** One time-slice of varying KPI values. */
    public static class DataEntry {
        /** Formatted as {@code yyyy-MM-dd'T'HH:mm}. */
        private String time;
        /** Only KPIs that vary over time. Static KPIs live in {@link PmDataCompactResponse#staticValues}. */
        private Map<String, Double> kpis;

        public DataEntry() {
        }

        public DataEntry(String time, Map<String, Double> kpis) {
            this.time = time;
            this.kpis = kpis;
        }

        public String getTime() { return time; }
        public void setTime(String time) { this.time = time; }

        public Map<String, Double> getKpis() { return kpis; }
        public void setKpis(Map<String, Double> kpis) { this.kpis = kpis; }
    }

    // -------------------------------------------------------------------------
    // Root accessors
    // -------------------------------------------------------------------------

    public Summary getSummary() { return summary; }
    public void setSummary(Summary summary) { this.summary = summary; }

    public Location getLocation() { return location; }
    public void setLocation(Location location) { this.location = location; }

    public List<DataEntry> getData() { return data; }
    public void setData(List<DataEntry> data) { this.data = data; }

    public Map<String, Double> getStaticValues() { return staticValues; }
    public void setStaticValues(Map<String, Double> staticValues) { this.staticValues = staticValues; }
}
