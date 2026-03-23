package org.springaicommunity.nova.pm.mapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springaicommunity.nova.pm.dto.PmDataPoint;
import org.springaicommunity.nova.pm.dto.PmDataResponse;
import org.springaicommunity.nova.pm.dto.TimeRange;
import org.springaicommunity.nova.pm.model.Granularity;
import org.springaicommunity.nova.pm.model.PmRecord;
import org.springframework.stereotype.Component;

/**
 * Maps Cassandra {@link PmRecord} entities to API DTOs.
 *
 * <p>Handles:
 * <ul>
 *   <li>KPI string-to-double conversion (null-safe)</li>
 *   <li>Optional KPI code filtering</li>
 *   <li>Response assembly with pagination metadata</li>
 * </ul>
 */
@Component
public class PmDataMapper {

    private static final Logger log = LoggerFactory.getLogger(PmDataMapper.class);

    /**
     * Maps a list of raw Cassandra records to a {@link PmDataResponse}.
     *
     * @param records     raw Cassandra entities
     * @param domain      request domain — echoed in the response
     * @param vendor      request vendor — echoed in the response
     * @param technology  request technology — echoed in the response
     * @param granularity request granularity — echoed in the response
     * @param dataLevel   request dataLevel — echoed in the response
     * @param nodeName    node name — echoed in the response
     * @param timeRange   request time range — echoed in the response
     * @param kpiCodes    optional KPI code filter; null/empty means return all
     * @param page        current page number
     * @param pageSize    page size used
     * @param hasMore     whether more pages are available
     * @return assembled response DTO
     */
    public PmDataResponse toResponse(
            List<PmRecord> records,
            String domain,
            String vendor,
            String technology,
            Granularity granularity,
            String dataLevel,
            String nodeName,
            TimeRange timeRange,
            Set<String> kpiCodes,
            int page,
            int pageSize,
            boolean hasMore) {

        List<PmDataPoint> dataPoints = new ArrayList<>(records.size());
        for (PmRecord record : records) {
            PmDataPoint point = toDataPoint(record, kpiCodes);
            dataPoints.add(point);
        }

        PmDataResponse response = new PmDataResponse();
        response.setDomain(domain);
        response.setVendor(vendor);
        response.setTechnology(technology);
        response.setGranularity(granularity);
        response.setDataLevel(dataLevel);
        response.setNodeName(nodeName);
        response.setTimeRange(timeRange);
        response.setData(dataPoints);
        response.setTotalPoints(dataPoints.size());
        response.setPage(page);
        response.setPageSize(pageSize);
        response.setHasMore(hasMore);
        return response;
    }

    private PmDataPoint toDataPoint(PmRecord record, Set<String> kpiCodes) {
        Map<String, Double> kpis = new LinkedHashMap<>();

        if (record.getKpiJson() != null) {
            record.getKpiJson().forEach((code, value) -> {
                if (kpiCodes == null || kpiCodes.isEmpty() || kpiCodes.contains(code)) {
                    kpis.put(code, parseDouble(code, value));
                }
            });
        }

        return new PmDataPoint(
                record.getKey().getTimestamp(),
                kpis,
                record.getMetaJson(),
                record.getNetworkType());
    }

    private Double parseDouble(String code, String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Double.parseDouble(value);
        }
        catch (NumberFormatException e) {
            log.warn("Non-numeric KPI value for code '{}': '{}' — returning null", code, value);
            return null;
        }
    }

}
