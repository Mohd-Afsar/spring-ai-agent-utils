package org.springaicommunity.nova.pm.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;

/**
 * Composite primary key for all PM Cassandra tables.
 *
 * <p>Partition key: (domain, vendor, technology, datalevel, date, nodename)
 * Clustering key: timestamp ASC
 *
 * <p>The partition groups all readings for one node on one calendar date,
 * keeping partitions bounded and timestamp range queries efficient.
 */
@PrimaryKeyClass
public class PmRecordKey implements Serializable {

    @PrimaryKeyColumn(name = "domain", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    private String domain;

    @PrimaryKeyColumn(name = "vendor", ordinal = 1, type = PrimaryKeyType.PARTITIONED)
    private String vendor;

    @PrimaryKeyColumn(name = "technology", ordinal = 2, type = PrimaryKeyType.PARTITIONED)
    private String technology;

    @PrimaryKeyColumn(name = "datalevel", ordinal = 3, type = PrimaryKeyType.PARTITIONED)
    private String dataLevel;

    @PrimaryKeyColumn(name = "date", ordinal = 4, type = PrimaryKeyType.PARTITIONED)
    private String date;

    @PrimaryKeyColumn(name = "nodename", ordinal = 5, type = PrimaryKeyType.PARTITIONED)
    private String nodeName;

    @PrimaryKeyColumn(name = "timestamp", ordinal = 6, type = PrimaryKeyType.CLUSTERED,
            ordering = Ordering.ASCENDING)
    private Instant timestamp;

    public PmRecordKey() {
    }

    public PmRecordKey(String domain, String vendor, String technology, String dataLevel,
            String date, String nodeName, Instant timestamp) {
        this.domain = domain;
        this.vendor = vendor;
        this.technology = technology;
        this.dataLevel = dataLevel;
        this.date = date;
        this.nodeName = nodeName;
        this.timestamp = timestamp;
    }

    public String getDomain() { return domain; }
    public void setDomain(String domain) { this.domain = domain; }

    public String getVendor() { return vendor; }
    public void setVendor(String vendor) { this.vendor = vendor; }

    public String getTechnology() { return technology; }
    public void setTechnology(String technology) { this.technology = technology; }

    public String getDataLevel() { return dataLevel; }
    public void setDataLevel(String dataLevel) { this.dataLevel = dataLevel; }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }

    public String getNodeName() { return nodeName; }
    public void setNodeName(String nodeName) { this.nodeName = nodeName; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PmRecordKey that)) return false;
        return Objects.equals(domain, that.domain)
                && Objects.equals(vendor, that.vendor)
                && Objects.equals(technology, that.technology)
                && Objects.equals(dataLevel, that.dataLevel)
                && Objects.equals(date, that.date)
                && Objects.equals(nodeName, that.nodeName)
                && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(domain, vendor, technology, dataLevel, date, nodeName, timestamp);
    }

}
