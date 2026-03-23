package org.springaicommunity.nova.pm.dto;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springaicommunity.nova.pm.dto.PmDataCompactResponse.DataEntry;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse.Location;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse.Summary;

/**
 * Concise PM data response with KPI formula metadata from MySQL.
 *
 * <p>Uses the same compact structure as {@link PmDataCompactResponse}
 * (summary, location, data, staticValues) plus {@code kpiDetails} and
 * {@code missingKpiCodes} from the KPI_FORMULA lookup.
 */
public class PmDataEnrichedResponse {

    private Summary summary;
    private Location location;
    private List<DataEntry> data;
    private Map<String, Double> staticValues;

    /** KPI_CODE → details from KPI_FORMULA (name, formula, counters, etc.). */
    private Map<String, KpiFormulaDetails> kpiDetails;

    /** KPI codes present in data but not found in KPI_FORMULA. */
    private Set<String> missingKpiCodes;

    public PmDataEnrichedResponse() {
        // no-arg constructor for JSON deserialisation
    }

    public Summary getSummary() { return summary; }
    public void setSummary(Summary summary) { this.summary = summary; }

    public Location getLocation() { return location; }
    public void setLocation(Location location) { this.location = location; }

    public List<DataEntry> getData() { return data; }
    public void setData(List<DataEntry> data) { this.data = data; }

    public Map<String, Double> getStaticValues() { return staticValues; }
    public void setStaticValues(Map<String, Double> staticValues) { this.staticValues = staticValues; }

    public Map<String, KpiFormulaDetails> getKpiDetails() { return kpiDetails; }
    public void setKpiDetails(Map<String, KpiFormulaDetails> kpiDetails) { this.kpiDetails = kpiDetails; }

    public Set<String> getMissingKpiCodes() { return missingKpiCodes; }
    public void setMissingKpiCodes(Set<String> missingKpiCodes) { this.missingKpiCodes = missingKpiCodes; }
}
