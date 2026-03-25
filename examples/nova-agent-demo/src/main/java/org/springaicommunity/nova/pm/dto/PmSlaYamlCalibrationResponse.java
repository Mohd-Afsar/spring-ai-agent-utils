package org.springaicommunity.nova.pm.dto;

import java.util.List;

/**
 * Result of calibrating SLA YAML from one or more {@link PmDataEnrichedResponse} payloads.
 */
public class PmSlaYamlCalibrationResponse {

    private String yaml;
    private int kpiCount;
    private int nodeCount;
    private List<String> nodeSummaries;
    private boolean appliedToRuntime;
    private boolean persistedToFile;
    private String persistedPath;
    private String message;

    public String getYaml() {
        return yaml;
    }

    public void setYaml(String yaml) {
        this.yaml = yaml;
    }

    public int getKpiCount() {
        return kpiCount;
    }

    public void setKpiCount(int kpiCount) {
        this.kpiCount = kpiCount;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public void setNodeCount(int nodeCount) {
        this.nodeCount = nodeCount;
    }

    public List<String> getNodeSummaries() {
        return nodeSummaries;
    }

    public void setNodeSummaries(List<String> nodeSummaries) {
        this.nodeSummaries = nodeSummaries;
    }

    public boolean isAppliedToRuntime() {
        return appliedToRuntime;
    }

    public void setAppliedToRuntime(boolean appliedToRuntime) {
        this.appliedToRuntime = appliedToRuntime;
    }

    public boolean isPersistedToFile() {
        return persistedToFile;
    }

    public void setPersistedToFile(boolean persistedToFile) {
        this.persistedToFile = persistedToFile;
    }

    public String getPersistedPath() {
        return persistedPath;
    }

    public void setPersistedPath(String persistedPath) {
        this.persistedPath = persistedPath;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
