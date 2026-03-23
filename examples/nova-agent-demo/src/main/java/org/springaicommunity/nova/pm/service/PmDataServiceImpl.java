package org.springaicommunity.nova.pm.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springaicommunity.nova.pm.dto.PmDataResponse;
import org.springaicommunity.nova.pm.dto.PmQueryRequest;
import org.springaicommunity.nova.pm.dto.TimeRange;
import org.springaicommunity.nova.pm.exception.PmNotFoundException;
import org.springaicommunity.nova.pm.mapper.PmDataMapper;
import org.springaicommunity.nova.pm.model.Granularity;
import org.springaicommunity.nova.pm.model.PmRecord;
import org.springaicommunity.nova.pm.repository.PmDataRepository;
import org.springaicommunity.nova.pm.util.DateRangeExpander;
import org.springaicommunity.nova.pm.util.GranularityTableResolver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Implementation of {@link PmDataService}.
 *
 * <p>Retrieval strategy:
 * <ol>
 *   <li>Resolve the Cassandra table from the requested granularity.</li>
 *   <li>Expand the time range into per-date partitions
 *       (Cassandra is partitioned by date).</li>
 *   <li>Issue one scoped query per date per node — always anchored to the
 *       full partition key to avoid full table scans.</li>
 *   <li>Merge results and apply pagination offset.</li>
 *   <li>Delegate formatting to the mapper — no analysis here.</li>
 * </ol>
 */
@Service
public class PmDataServiceImpl implements PmDataService {

    private static final Logger log = LoggerFactory.getLogger(PmDataServiceImpl.class);

    private final PmDataRepository repository;
    private final GranularityTableResolver tableResolver;
    private final DateRangeExpander dateExpander;
    private final PmDataMapper mapper;

    @Value("${pm.query.default-page-size:500}")
    private int defaultPageSize;

    @Value("${pm.query.max-result-size:10000}")
    private int maxResultSize;

    public PmDataServiceImpl(PmDataRepository repository,
            GranularityTableResolver tableResolver,
            DateRangeExpander dateExpander,
            PmDataMapper mapper) {
        this.repository = repository;
        this.tableResolver = tableResolver;
        this.dateExpander = dateExpander;
        this.mapper = mapper;
    }

    @Override
    public PmDataResponse getData(String domain, String vendor, String technology,
            String dataLevel, String nodeName, Granularity granularity,
            Instant from, Instant to, Set<String> kpiCodes, int page, int size) {

        String tableName = null;
        try {
         tableName = tableResolver.resolve(granularity);
            log.info("Resolved table name for granularity '{}' to '{}'", granularity, tableName);
        String effectiveDataLevel = dataLevel != null ? dataLevel : "NODE";
        int effectiveSize = size > 0 ? size : defaultPageSize;

        List<PmRecord> records = fetchRecords(tableName, domain, vendor, technology,
                effectiveDataLevel, nodeName, from, to, page, effectiveSize);

        if (records.isEmpty()) {
            throw new PmNotFoundException(String.format(
                    "No PM data found for node '%s' in table '%s' between %s and %s",
                    nodeName, tableName, from, to));
        }

        boolean hasMore = records.size() == effectiveSize;

        TimeRange timeRange = new TimeRange(from, to);
        return mapper.toResponse(records, domain, vendor, technology, granularity,
                effectiveDataLevel, nodeName, timeRange, kpiCodes, page, effectiveSize, hasMore);
        } catch (Exception e) {
            log.error("Error getting PM data for node '{}' in table '{}' between {} and {}", nodeName, tableName, from, to, e);
            throw e;
        }
    }

    @Override
    public List<PmDataResponse> queryData(PmQueryRequest request) {
        String tableName = tableResolver.resolve(request.getGranularity());
        String effectiveDataLevel = request.getDataLevel() != null ? request.getDataLevel() : "NODE";
        Instant from = request.getTimeRange().getFrom();
        Instant to = request.getTimeRange().getTo();

        List<String> nodeNames = request.getNodeNames();
        if (nodeNames == null || nodeNames.isEmpty()) {
            List<String> dates = dateExpander.expand(from, to);
            Set<String> discovered = repository.findDistinctNodeNames(
                    tableName, request.getDomain(), request.getVendor(),
                    request.getTechnology(), effectiveDataLevel, dates);
            nodeNames = discovered.stream()
                    .filter(n -> n != null && !n.isBlank())
                    .toList();
            log.info("Auto-discovered {} nodes for query", nodeNames.size());
            if (nodeNames.isEmpty()) {
                return List.of();
            }
        }

        List<PmDataResponse> responses = new ArrayList<>();
        Map<String, List<PmRecord>> recordsByNode = fetchRecordsForNodes(tableName, request.getDomain(),
                request.getVendor(), request.getTechnology(), effectiveDataLevel, nodeNames, from, to,
                request.getPage(), request.getSize());
        TimeRange timeRange = new TimeRange(from, to);
        for (String nodeName : nodeNames) {
            List<PmRecord> records = recordsByNode.getOrDefault(nodeName, List.of());
            boolean hasMore = records.size() == request.getSize();
            PmDataResponse response = mapper.toResponse(records, request.getDomain(),
                    request.getVendor(), request.getTechnology(), request.getGranularity(),
                    effectiveDataLevel, nodeName, timeRange,
                    request.getKpiCodes(), request.getPage(), request.getSize(), hasMore);
            responses.add(response);
        }

        return responses;
    }

    @Override
    public SortedSet<String> discoverNodes(String domain, String vendor, String technology,
            String dataLevel, Granularity granularity, Instant from, Instant to) {
        String tableName = tableResolver.resolve(granularity);
        String effectiveDataLevel = dataLevel != null ? dataLevel : "NODE";
        List<String> dates = dateExpander.expand(from, to);
        Set<String> discovered = repository.findDistinctNodeNames(
                tableName, domain, vendor, technology, effectiveDataLevel, dates);
        return new TreeSet<>(discovered);
    }

    /**
     * Fetches records across all date partitions covered by the time range,
     * then applies skip (page * size) and limit (size) in memory.
     *
     * <p>Each per-date query is bounded by the full partition key + timestamp range.
     * No full table scans are issued.
     */
    private List<PmRecord> fetchRecords(String tableName, String domain, String vendor,
            String technology, String dataLevel, String nodeName,
            Instant from, Instant to, int page, int size) {

        List<String> dates = dateExpander.expand(from, to);
        int skip = page * size;
        int fetchLimit = skip + size + 1; // +1 to detect hasMore

        List<PmRecord> allRecords = new ArrayList<>();

        for (String date : dates) {
            if (allRecords.size() >= fetchLimit) {
                break;
            }
            int remaining = fetchLimit - allRecords.size();
            log.debug("Querying table={} node={} date={} limit={}", tableName, nodeName, date, remaining);

            List<PmRecord> dateRecords = repository.findByPartitionAndTimeRange(
                    tableName, domain, vendor, technology, dataLevel,
                    date, nodeName, from, to, remaining);

            log.info("Fetched {} records for table={} node={} date={}", dateRecords!=null ? dateRecords.size() : 0, tableName, nodeName, date);
            allRecords.addAll(dateRecords);
        }

        // Apply pagination offset
        if (skip >= allRecords.size()) {
            return List.of();
        }
        int end = Math.min(skip + size, allRecords.size());
        return allRecords.subList(skip, end);
    }

    private Map<String, List<PmRecord>> fetchRecordsForNodes(String tableName, String domain, String vendor,
            String technology, String dataLevel, List<String> nodeNames,
            Instant from, Instant to, int page, int size) {
        Map<String, List<PmRecord>> grouped = new LinkedHashMap<>();
        if (nodeNames == null || nodeNames.isEmpty()) return grouped;
        for (String node : nodeNames) {
            grouped.put(node, new ArrayList<>());
        }

        List<String> dates = dateExpander.expand(from, to);
        int skipPerNode = page * size;
        int fetchPerNode = skipPerNode + size + 1;
        int batchLimit = Math.max(1, fetchPerNode * Math.max(1, nodeNames.size()));

        for (String date : dates) {
            List<PmRecord> dateRecords = repository.findByPartitionAndTimeRangeForNodeNames(
                    tableName, domain, vendor, technology, dataLevel, date, nodeNames, from, to, batchLimit);
            for (PmRecord r : dateRecords) {
                if (r == null || r.getKey() == null) continue;
                String node = r.getKey().getNodeName();
                List<PmRecord> bucket = grouped.get(node);
                if (bucket != null && bucket.size() < fetchPerNode) {
                    bucket.add(r);
                }
            }
        }

        Map<String, List<PmRecord>> paged = new LinkedHashMap<>();
        for (Map.Entry<String, List<PmRecord>> e : grouped.entrySet()) {
            List<PmRecord> records = e.getValue();
            if (skipPerNode >= records.size()) {
                paged.put(e.getKey(), List.of());
                continue;
            }
            int end = Math.min(skipPerNode + size, records.size());
            paged.put(e.getKey(), records.subList(skipPerNode, end));
        }
        return paged;
    }

}
