package org.springaicommunity.nova.pm.dto;

import java.util.List;
import java.util.Set;

import org.springaicommunity.nova.pm.model.Granularity;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Request body for POST /pm/data/query.
 * Supports multi-node queries and optional KPI filtering.
 */
public class PmQueryRequest {

    @NotBlank(message = "domain is required")
    private String domain;

    @NotBlank(message = "vendor is required")
    private String vendor;

    @NotBlank(message = "technology is required")
    private String technology;

    private String dataLevel;

    /**
     * One or more node names to query.
     * If null or empty, all nodes are auto-discovered for the given
     * domain / vendor / technology / dataLevel and time window.
     */
    private List<String> nodeNames;

    @NotNull(message = "granularity is required")
    private Granularity granularity;

    @NotNull(message = "timeRange is required")
    @Valid
    private TimeRange timeRange;

    /**
     * Optional set of KPI codes to return.
     * If null or empty, all KPIs are returned.
     */
    private Set<String> kpiCodes;

    /** Page number (0-based). Defaults to 0. */
    private int page = 0;

    /** Results per page. Defaults to configured default. */
    private int size = 500;

    public String getDomain() { return domain; }
    public void setDomain(String domain) { this.domain = domain; }

    public String getVendor() { return vendor; }
    public void setVendor(String vendor) { this.vendor = vendor; }

    public String getTechnology() { return technology; }
    public void setTechnology(String technology) { this.technology = technology; }

    public String getDataLevel() { return dataLevel; }
    public void setDataLevel(String dataLevel) { this.dataLevel = dataLevel; }

    public List<String> getNodeNames() { return nodeNames; }
    public void setNodeNames(List<String> nodeNames) { this.nodeNames = nodeNames; }

    public Granularity getGranularity() { return granularity; }
    public void setGranularity(Granularity granularity) { this.granularity = granularity; }

    public TimeRange getTimeRange() { return timeRange; }
    public void setTimeRange(TimeRange timeRange) { this.timeRange = timeRange; }

    public Set<String> getKpiCodes() { return kpiCodes; }
    public void setKpiCodes(Set<String> kpiCodes) { this.kpiCodes = kpiCodes; }

    public int getPage() { return page; }
    public void setPage(int page) { this.page = page; }

    public int getSize() { return size; }
    public void setSize(int size) { this.size = size; }

}
