package org.springaicommunity.nova.pm.service;

import org.springaicommunity.nova.pm.dto.PmDataResponse;
import org.springaicommunity.nova.pm.dto.PmQueryRequest;
import org.springaicommunity.nova.pm.model.Granularity;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

/**
 * Service contract for PM KPI data retrieval.
 * Implementations must NOT perform KPI computation or analytics.
 */
public interface PmDataService {

    /**
     * Retrieves PM data for a single node using GET-style parameters.
     *
     * @param domain      network domain
     * @param vendor      equipment vendor
     * @param technology  technology type
     * @param dataLevel   aggregation level / geography level
     * @param nodeName    network node identifier
     * @param granularity time granularity determining the Cassandra table
     * @param from        start of the time range (inclusive)
     * @param to          end of the time range (inclusive)
     * @param kpiCodes    optional set of KPI codes to filter; null returns all
     * @param page        page number (0-based)
     * @param size        page size
     * @return structured PM data response ready for AI agent consumption
     */
    PmDataResponse getData(String domain, String vendor, String technology,
            String dataLevel, String nodeName, Granularity granularity,
            Instant from, Instant to, Set<String> kpiCodes, int page, int size);

    /**
     * Retrieves PM data for one or more nodes using a structured query request.
     * Results for each node are returned as a separate response object in the list.
     *
     * @param request validated query request
     * @return one response per requested node
     */
    List<PmDataResponse> queryData(PmQueryRequest request);

    /**
     * Discovers distinct node names available for the given criteria and time window.
     *
     * <p>Queries Cassandra partition keys — no full table scan is performed.
     *
     * @param domain      network domain
     * @param vendor      equipment vendor
     * @param technology  technology type
     * @param dataLevel   aggregation level (null defaults to "NODE")
     * @param granularity determines which Cassandra table to query
     * @param from        window start (used to derive date partitions)
     * @param to          window end
     * @return sorted set of node names present in the given window
     */
    SortedSet<String> discoverNodes(String domain, String vendor, String technology,
            String dataLevel, Granularity granularity, Instant from, Instant to);

}
