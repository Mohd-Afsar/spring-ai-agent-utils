package org.springaicommunity.nova.pm.service;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import org.springaicommunity.nova.pm.dto.KpiFormulaDetails;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse;
import org.springaicommunity.nova.pm.dto.PmDataCompactResponse.DataEntry;
import org.springaicommunity.nova.pm.dto.PmDataEnrichedResponse;
import org.springaicommunity.nova.pm.dto.PmDataResponse;
import org.springaicommunity.nova.pm.mapper.PmCompactMapper;
import org.springaicommunity.nova.pm.repository.KpiFormulaRepository;

/**
 * Enriches PM data with KPI formula metadata from MySQL, in concise form.
 *
 * <p>Produces a compact payload (summary, location, data, staticValues) plus
 * {@code kpiDetails} from {@code KPI_FORMULA} and {@code missingKpiCodes}.
 */
@Service
public class PmDataEnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(PmDataEnrichmentService.class);
    private final KpiFormulaRepository kpiFormulaRepository;
    private final PmCompactMapper compactMapper;

    public PmDataEnrichmentService(KpiFormulaRepository kpiFormulaRepository,
            PmCompactMapper compactMapper) {
        this.kpiFormulaRepository = kpiFormulaRepository;
        this.compactMapper = compactMapper;
    }

    /**
     * Enriches a PM data response: converts to compact shape and attaches
     * KPI details from MySQL (KPI_FORMULA, DELETED=0).
     *
     * @param pmDataResponse response from {@link PmDataService#getData} or
     *                       {@link PmDataService#queryData}
     * @return concise enriched response (summary, location, data, staticValues,
     *         kpiDetails, missingKpiCodes)
     */
    public PmDataEnrichedResponse enrich(PmDataResponse pmDataResponse) {
        try {
        PmDataCompactResponse compact = compactMapper.toCompact(pmDataResponse);
        Set<String> allCodes = extractKpiCodesFromCompact(compact);
        Map<String, KpiFormulaDetails> kpiDetails = kpiFormulaRepository.findByKpiCodeIn(allCodes);
        log.info("Found {} KPI details for {} codes", kpiDetails!=null ? kpiDetails.size() : 0, allCodes.size());
        Set<String> missingKpiCodes = new HashSet<>(allCodes);
        missingKpiCodes.removeAll(kpiDetails.keySet());

        PmDataEnrichedResponse enriched = new PmDataEnrichedResponse();
        enriched.setSummary(compact.getSummary());
        enriched.setLocation(compact.getLocation());
        enriched.setData(compact.getData());
        enriched.setStaticValues(compact.getStaticValues());
        enriched.setKpiDetails(kpiDetails.isEmpty() ? null : kpiDetails);
        enriched.setMissingKpiCodes(missingKpiCodes.isEmpty() ? null : missingKpiCodes);
        return enriched;
        } catch (Exception e) {
            log.error("Error enriching PM data response", e);
            throw e;
        }
    }

    private Set<String> extractKpiCodesFromCompact(PmDataCompactResponse compact) {
        Set<String> codes = new HashSet<>();
        if (compact.getStaticValues() != null) {
            codes.addAll(compact.getStaticValues().keySet());
        }
        if (compact.getData() != null) {
            compact.getData().stream()
                    .map(DataEntry::getKpis)
                    .filter(kpis -> kpis != null && !kpis.isEmpty())
                    .flatMap(kpis -> kpis.keySet().stream())
                    .forEach(codes::add);
        }
        return codes;
    }
}
