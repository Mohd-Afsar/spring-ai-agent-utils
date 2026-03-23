package org.springaicommunity.nova.pm.controller;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import org.springaicommunity.nova.pm.dto.PmDataCompactResponse;
import org.springaicommunity.nova.pm.dto.PmDataEnrichedResponse;
import org.springaicommunity.nova.pm.dto.PmDataResponse;
import org.springaicommunity.nova.pm.dto.PmQueryRequest;
import org.springaicommunity.nova.pm.mapper.PmCompactMapper;
import org.springaicommunity.nova.pm.model.Granularity;
import org.springaicommunity.nova.pm.service.PmDataEnrichmentService;
import org.springaicommunity.nova.pm.service.PmDataService;
import org.springaicommunity.nova.pm.util.PmQueryValidator;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

/**
 * REST API for PM KPI data retrieval.
 *
 * <p>Designed for consumption by downstream AI agents (NOVA sub-agents) that
 * perform analysis and reporting. This controller retrieves data only —
 * no KPI computation or analytics are performed here.
 *
 * <h2>Endpoints</h2>
 * <ul>
 *   <li>{@code GET /pm/data} — single-node query via query parameters</li>
 *   <li>{@code POST /pm/data/query} — structured query supporting multiple nodes</li>
 * </ul>
 *
 * <h2>Example GET request</h2>
 * <pre>
 * GET /pm/data
 *   ?domain=TRANSPORT
 *   &vendor=NOKIA
 *   &technology=COMMON
 *   &dataLevel=NODE
 *   &nodeName=CNB-MX-204
 *   &granularity=HOURLY
 *   &from=2024-06-01T00:00:00Z
 *   &to=2024-06-01T23:59:59Z
 *   &size=100
 * </pre>
 *
 * <h2>Example POST request body</h2>
 * <pre>
 * {
 *   "domain": "TRANSPORT",
 *   "vendor": "NOKIA",
 *   "technology": "COMMON",
 *   "dataLevel": "NODE",
 *   "nodeNames": ["CNB-MX-204", "CNB-MX-205"],
 *   "granularity": "HOURLY",
 *   "timeRange": { "from": "2024-06-01T00:00:00Z", "to": "2024-06-01T23:59:59Z" },
 *   "kpiCodes": ["TX_UTIL", "RX_UTIL"],
 *   "page": 0,
 *   "size": 200
 * }
 * </pre>
 */
@RestController
@RequestMapping("/pm/data")
@Validated
public class PmDataController {

    private final PmDataService pmDataService;
    private final PmQueryValidator validator;
    private final PmCompactMapper compactMapper;
    private final PmDataEnrichmentService enrichmentService;

    public PmDataController(PmDataService pmDataService, PmQueryValidator validator,
            PmCompactMapper compactMapper, PmDataEnrichmentService enrichmentService) {
        this.pmDataService = pmDataService;
        this.validator = validator;
        this.compactMapper = compactMapper;
        this.enrichmentService = enrichmentService;
    }

    /**
     * Single-node PM data retrieval via GET.
     *
     * @param domain      network domain (e.g. TRANSPORT)
     * @param vendor      equipment vendor (e.g. NOKIA)
     * @param technology  technology type (e.g. COMMON)
     * @param dataLevel   aggregation / geography level (optional, default NODE)
     * @param nodeName    network node identifier
     * @param granularity time granularity
     * @param from        range start (ISO-8601 instant)
     * @param to          range end (ISO-8601 instant)
     * @param kpiCodes    optional KPI code filter (comma-separated)
     * @param page        page number, 0-based (default 0)
     * @param size        page size (default 500)
     * @return PM data response
     */
    @GetMapping
    public ResponseEntity<PmDataResponse> getData(
            @RequestParam String domain,
            @RequestParam String vendor,
            @RequestParam String technology,
            @RequestParam(required = false) String dataLevel,
            @RequestParam(required = false) String nodeName,
            @RequestParam @NotNull Granularity granularity,
            @RequestParam @NotNull Instant from,
            @RequestParam @NotNull Instant to,
            @RequestParam(required = false) Set<String> kpiCodes,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "500") int size) {

        validator.validateGetParams(domain, vendor, technology, nodeName, from, to, size);

        PmDataResponse response = pmDataService.getData(
                domain, vendor, technology, dataLevel, nodeName,
                granularity, from, to, kpiCodes, page, size);

        return ResponseEntity.ok(response);
    }

    /**
     * Multi-node PM data retrieval via POST.
     * Returns one {@link PmDataResponse} per requested node.
     *
     * @param request validated query request body
     * @return list of PM data responses, one per node
     */
    @PostMapping("/query")
    public ResponseEntity<List<PmDataResponse>> queryData(
            @RequestBody @Valid PmQueryRequest request) {

        validator.validate(request);

        List<PmDataResponse> responses = pmDataService.queryData(request);
        return ResponseEntity.ok(responses);
    }

    /**
     * Single-node PM data with KPI formula details from MySQL.
     *
     * <p>Same parameters as {@code GET /pm/data}. Response includes {@code pmData}
     * plus {@code kpiDetails} (KPI_CODE → name, formula, counters, etc. from
     * KPI_FORMULA) and {@code missingKpiCodes} for codes not found in MySQL.
     */
    @GetMapping("/enriched")
    public ResponseEntity<PmDataEnrichedResponse> getEnrichedData(
            @RequestParam String domain,
            @RequestParam String vendor,
            @RequestParam String technology,
            @RequestParam(required = false) String dataLevel,
            @RequestParam(required = false) String nodeName,
            @RequestParam @NotNull Granularity granularity,
            @RequestParam @NotNull Instant from,
            @RequestParam @NotNull Instant to,
            @RequestParam(required = false) Set<String> kpiCodes,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "500") int size) {

        validator.validateGetParams(domain, vendor, technology, nodeName, from, to, size);

        PmDataResponse pmData = pmDataService.getData(
                domain, vendor, technology, dataLevel, nodeName,
                granularity, from, to, kpiCodes, page, size);

        return ResponseEntity.ok(enrichmentService.enrich(pmData));
    }

    /**
     * Multi-node PM data with KPI formula details from MySQL.
     *
     * <p>Same body as {@code POST /pm/data/query}; returns one enriched response
     * per requested node.
     */
    @PostMapping("/query/enriched")
    public ResponseEntity<List<PmDataEnrichedResponse>> queryEnrichedData(
            @RequestBody @Valid PmQueryRequest request) {

        validator.validate(request);

        List<PmDataResponse> responses = pmDataService.queryData(request);
        List<PmDataEnrichedResponse> enrichedList = responses.stream()
                .map(enrichmentService::enrich)
                .toList();
        return ResponseEntity.ok(enrichedList);
    }

    /**
     * Compact single-node PM data — optimised for LLM token efficiency.
     *
     * <p>Same parameters as {@code GET /pm/data} but the response is structured
     * with a {@code summary}, {@code location}, time-varying {@code data} entries,
     * and a {@code staticValues} map for KPIs that never change across the window.
     *
     * <p>Example:
     * <pre>
     * GET /pm/data/compact
     *   ?domain=TRANSPORT&amp;vendor=JUNIPER&amp;technology=COMMON
     *   &amp;dataLevel=ROUTER_COMMON_Router&amp;nodeName=172.31.31.131
     *   &amp;granularity=HOURLY&amp;from=2025-01-19T00:00:00Z&amp;to=2025-01-23T23:59:59Z
     * </pre>
     */
    @GetMapping("/compact")
    public ResponseEntity<PmDataCompactResponse> getCompactData(
            @RequestParam String domain,
            @RequestParam String vendor,
            @RequestParam String technology,
            @RequestParam(required = false) String dataLevel,
            @RequestParam(required = false) String nodeName,
            @RequestParam @NotNull Granularity granularity,
            @RequestParam @NotNull Instant from,
            @RequestParam @NotNull Instant to,
            @RequestParam(required = false) Set<String> kpiCodes,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "500") int size) {

        validator.validateGetParams(domain, vendor, technology, nodeName, from, to, size);

        PmDataResponse full = pmDataService.getData(
                domain, vendor, technology, dataLevel, nodeName,
                granularity, from, to, kpiCodes, page, size);

        return ResponseEntity.ok(compactMapper.toCompact(full));
    }

    /**
     * Compact multi-node PM data — optimised for LLM token efficiency.
     *
     * <p>Same body as {@code POST /pm/data/query}; returns one compact response
     * per requested node.
     */
    @PostMapping("/query/compact")
    public ResponseEntity<List<PmDataCompactResponse>> queryCompactData(
            @RequestBody @Valid PmQueryRequest request) {

        validator.validate(request);

        List<PmDataResponse> responses = pmDataService.queryData(request);
        List<PmDataCompactResponse> compactList = responses.stream()
                .map(compactMapper::toCompact)
                .toList();
        return ResponseEntity.ok(compactList);
    }

    /**
     * Node discovery — returns the distinct node names available for the
     * requested domain / vendor / technology / dataLevel in a given time window.
     *
     * <p>Uses Cassandra {@code SELECT DISTINCT} on partition keys; no full table
     * scan is performed. Use the returned list as input to
     * {@code POST /pm/data/query/enriched} to fetch data for all nodes.
     *
     * <p>Example:
     * <pre>
     * GET /pm/nodes
     *   ?domain=TRANSPORT&amp;vendor=JUNIPER&amp;technology=COMMON
     *   &amp;dataLevel=ROUTER_COMMON_Router&amp;granularity=HOURLY
     *   &amp;from=2025-01-23T00:00:00Z&amp;to=2025-01-23T01:00:00Z
     * </pre>
     */
    @GetMapping("/nodes")
    public ResponseEntity<SortedSet<String>> getNodes(
            @RequestParam String domain,
            @RequestParam String vendor,
            @RequestParam String technology,
            @RequestParam(required = false) String dataLevel,
            @RequestParam @NotNull Granularity granularity,
            @RequestParam @NotNull Instant from,
            @RequestParam @NotNull Instant to) {

        SortedSet<String> nodes = pmDataService.discoverNodes(
                domain, vendor, technology, dataLevel, granularity, from, to);
        return ResponseEntity.ok(nodes);
    }

}
