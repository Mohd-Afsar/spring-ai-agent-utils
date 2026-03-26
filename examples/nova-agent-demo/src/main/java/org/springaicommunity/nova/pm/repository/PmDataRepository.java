package org.springaicommunity.nova.pm.repository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springaicommunity.nova.pm.model.PmRecord;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Repository for PM KPI data retrieval from Cassandra.
 *
 * <p>Uses {@link CassandraTemplate}'s {@code ExecutableSelectOperation} API
 * ({@code .query().inTable().matching().all()}) so the target Cassandra table
 * can be specified at query time. This is necessary because PM data is stored
 * in separate pre-aggregated tables per granularity level
 * (e.g. {@code combinehourlypm}, {@code combinedailypm}).
 *
 * <p>Every query is anchored to the full partition key
 * (domain + vendor + technology + datalevel + date + nodename) before applying
 * the timestamp clustering-key range filter. This prevents full table scans.
 */
@Repository
public class PmDataRepository {

    private final CassandraTemplate cassandraTemplate;
    private final CqlSession cqlSession;
    private final JdbcTemplate jdbcTemplate;

    private static final Logger log = LoggerFactory.getLogger(PmDataRepository.class);

    public PmDataRepository(CassandraTemplate cassandraTemplate, CqlSession cqlSession, JdbcTemplate jdbcTemplate) {
        this.cassandraTemplate = cassandraTemplate;
        this.cqlSession = cqlSession;
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Retrieves PM records for a single node within one date partition.
     *
     * <p>The caller must split multi-day time ranges into per-date calls using
     * {@link org.springaicommunity.nova.pm.util.DateRangeExpander}, because the
     * Cassandra partition key includes the date field.
     *
     * @param tableName  Cassandra table resolved from granularity (e.g. "combinehourlypm")
     * @param domain     partition key field
     * @param vendor     partition key field
     * @param technology partition key field
     * @param dataLevel  partition key field (e.g. "ROUTER_COMMON_Router")
     * @param date       partition key field — yyyyMMdd format (e.g. "20250623")
     * @param nodeName   partition key field (node IP or name)
     * @param from       timestamp range start (inclusive)
     * @param to         timestamp range end (inclusive)
     * @param limit      maximum rows to return
     * @return matching PM records, ordered by timestamp ascending
     */
    public List<PmRecord> findByPartitionAndTimeRange(
            String tableName,
            String domain,
            String vendor,
            String technology,
            String dataLevel,
            String date,
            String nodeName,
            Instant from,
            Instant to,
            int limit) {

        logCassandraCqlSingle(tableName, domain, vendor, technology, dataLevel, date, nodeName, from, to, limit);
        Query query = Query.query(
                Criteria.where("domain").is(domain),
                Criteria.where("vendor").is(vendor),
                Criteria.where("technology").is(technology),
                Criteria.where("datalevel").is(dataLevel),
                Criteria.where("date").is(date),
                Criteria.where("nodename").is(nodeName),
                Criteria.where("timestamp").gte(from),
                Criteria.where("timestamp").lte(to))
            .limit(limit);
        log.info("Query: {}", query.toString());
        return cassandraTemplate.query(PmRecord.class)
            .inTable(tableName)
            .matching(query)
            .all();
    }

    /**
     * Retrieves PM records for multiple node names in a single Cassandra query using IN(nodename).
     */
    public List<PmRecord> findByPartitionAndTimeRangeForNodeNames(
            String tableName,
            String domain,
            String vendor,
            String technology,
            String dataLevel,
            String date,
            List<String> nodeNames,
            Instant from,
            Instant to,
            int limit) {

        if (nodeNames == null || nodeNames.isEmpty()) {
            return List.of();
        }
        List<String> cleaned = nodeNames.stream()
                .filter(n -> n != null && !n.isBlank())
                .distinct()
                .toList();
        if (cleaned.isEmpty()) {
            return List.of();
        }

        logCassandraCqlMulti(tableName, domain, vendor, technology, dataLevel, date, cleaned, from, to, limit);
        Query query = Query.query(
                Criteria.where("domain").is(domain),
                Criteria.where("vendor").is(vendor),
                Criteria.where("technology").is(technology),
                Criteria.where("datalevel").is(dataLevel),
                Criteria.where("date").is(date),
                Criteria.where("nodename").in(new ArrayList<>(cleaned)),
                Criteria.where("timestamp").gte(from),
                Criteria.where("timestamp").lte(to))
            .limit(limit);
        log.info("Batch Query (nodes={}): {}", cleaned.size(), query);
        return cassandraTemplate.query(PmRecord.class)
                .inTable(tableName)
                .matching(query)
                .all();
    }

    /**
     * Returns distinct node names ({@code NE_ID}) from MySQL {@code NETWORK_ELEMENT}
     * for the supplied domain, vendor, technology, and dataLevel ({@code NE_TYPE}).
     *
     * @param tableName   ignored (kept for API compatibility with Cassandra-era callers)
     * @param domain      {@code DOMAIN}
     * @param vendor      {@code VENDOR}
     * @param technology  {@code TECHNOLOGY}
     * @param dataLevel   {@code NE_TYPE}
     * @param dates       ignored
     * @return distinct {@code NE_ID} values, insertion order preserved
     */
    public Set<String> findDistinctNodeNames(
            String tableName,
            String domain,
            String vendor,
            String technology,
            String dataLevel,
            List<String> dates) {

        String sql = "SELECT DISTINCT NE_ID FROM NETWORK_ELEMENT WHERE DOMAIN = ? AND VENDOR = ? AND NE_TYPE = ? AND TECHNOLOGY = ?";
        Set<String> nodeNames = new LinkedHashSet<>();
        try {
            List<String> result = jdbcTemplate.queryForList(
                    sql, String.class, domain, vendor, "ROUTER", technology);
            for (String name : result) {
                if (name != null && !name.isBlank()) {
                    nodeNames.add(name);
                }
            }
            log.info("Discovered {} distinct NE_IDs in NETWORK_ELEMENT for {}/{}/{}/{}", 
                nodeNames.size(), domain, vendor, dataLevel, technology);
        } catch (Exception e) {
            log.error("Error querying distinct NE_IDs from NETWORK_ELEMENT", e);
        }
        return nodeNames;
    }

    /**
     * Counts matching records for a single partition and time range.
     * Used to determine whether more pages are available without fetching all data.
     */
    public long countByPartitionAndTimeRange(
            String tableName,
            String domain,
            String vendor,
            String technology,
            String dataLevel,
            String date,
            String nodeName,
            Instant from,
            Instant to) {

        logCassandraCqlSingle(tableName, domain, vendor, technology, dataLevel, date, nodeName, from, to, -1);
        Query query = Query.query(
                Criteria.where("domain").is(domain),
                Criteria.where("vendor").is(vendor),
                Criteria.where("technology").is(technology),
                Criteria.where("datalevel").is(dataLevel),
                Criteria.where("date").is(date),
                Criteria.where("nodename").is(nodeName),
                Criteria.where("timestamp").gte(from),
                Criteria.where("timestamp").lte(to));

        return cassandraTemplate.query(PmRecord.class)
            .inTable(tableName)
            .matching(query)
            .count();
    }

    private void logCassandraCqlSingle(String tableName, String domain, String vendor, String technology,
            String dataLevel, String date, String nodeName, Instant from, Instant to, int limit) {
        String cql = "SELECT * FROM " + tableName
                + " WHERE domain=? AND vendor=? AND technology=? AND datalevel=? AND date=? AND nodename=?"
                + " AND timestamp>=? AND timestamp<=?"
                + (limit > 0 ? " LIMIT ?" : "");
        log.info("========cassandra_cql start=========\n{}\nparams=[domain={}, vendor={}, technology={}, datalevel={}, date={}, nodename={}, from={}, to={}{}]\n========cassandra_cql end=========",
                cql, domain, vendor, technology, dataLevel, date, nodeName, from, to, (limit > 0 ? ", limit=" + limit : ""));
    }

    private void logCassandraCqlMulti(String tableName, String domain, String vendor, String technology,
            String dataLevel, String date, List<String> nodeNames, Instant from, Instant to, int limit) {
        String cql = "SELECT * FROM " + tableName
                + " WHERE domain=? AND vendor=? AND technology=? AND datalevel=? AND date=? AND nodename IN ?"
                + " AND timestamp>=? AND timestamp<=?"
                + (limit > 0 ? " LIMIT ?" : "");
        log.info("========cassandra_cql start=========\n{}\nparams=[domain={}, vendor={}, technology={}, datalevel={}, date={}, nodeNames(size)={}, from={}, to={}{}]\n========cassandra_cql end=========",
                cql, domain, vendor, technology, dataLevel, date, (nodeNames == null ? 0 : nodeNames.size()), from, to,
                (limit > 0 ? ", limit=" + limit : ""));
    }

}
