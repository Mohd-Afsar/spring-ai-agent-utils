package org.springaicommunity.nova.pm.dto;

/**
 * KPI metadata fetched from MySQL KPI_FORMULA table, keyed by KPI_CODE.
 * Designed to be attached to PM data responses for LLM consumption.
 */
public class KpiFormulaDetails {

    private String kpiCode;
    private String kpiName;
    private String kpiUnit;
    private String description;
    private String formulaCounterInfo;

    private String domain;
    private String vendor;
    private String technology;
    private String aggregationLevel;
    private String kpiNodeAggregation;
    private String kpiTimeAggregation;
    private String moType;
    private String kpiType;
    private String accessType;
    private String kpiGroup;

    public KpiFormulaDetails() {
    }

    public String getKpiCode() { return kpiCode; }
    public void setKpiCode(String kpiCode) { this.kpiCode = kpiCode; }

    public String getKpiName() { return kpiName; }
    public void setKpiName(String kpiName) { this.kpiName = kpiName; }

    public String getKpiUnit() { return kpiUnit; }
    public void setKpiUnit(String kpiUnit) { this.kpiUnit = kpiUnit; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getFormulaCounterInfo() { return formulaCounterInfo; }
    public void setFormulaCounterInfo(String formulaCounterInfo) { this.formulaCounterInfo = formulaCounterInfo; }

    public String getDomain() { return domain; }
    public void setDomain(String domain) { this.domain = domain; }

    public String getVendor() { return vendor; }
    public void setVendor(String vendor) { this.vendor = vendor; }

    public String getTechnology() { return technology; }
    public void setTechnology(String technology) { this.technology = technology; }

    public String getAggregationLevel() { return aggregationLevel; }
    public void setAggregationLevel(String aggregationLevel) { this.aggregationLevel = aggregationLevel; }

    public String getKpiNodeAggregation() { return kpiNodeAggregation; }
    public void setKpiNodeAggregation(String kpiNodeAggregation) { this.kpiNodeAggregation = kpiNodeAggregation; }

    public String getKpiTimeAggregation() { return kpiTimeAggregation; }
    public void setKpiTimeAggregation(String kpiTimeAggregation) { this.kpiTimeAggregation = kpiTimeAggregation; }

    public String getMoType() { return moType; }
    public void setMoType(String moType) { this.moType = moType; }

    public String getKpiType() { return kpiType; }
    public void setKpiType(String kpiType) { this.kpiType = kpiType; }

    public String getAccessType() { return accessType; }
    public void setAccessType(String accessType) { this.accessType = accessType; }

    public String getKpiGroup() { return kpiGroup; }
    public void setKpiGroup(String kpiGroup) { this.kpiGroup = kpiGroup; }
}

