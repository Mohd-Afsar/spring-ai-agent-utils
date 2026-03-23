package org.springaicommunity.nova.pm.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import org.springaicommunity.nova.pm.dto.KpiFormulaDetails;

/**
 * JDBC repository for KPI formula metadata from MySQL {@code KPI_FORMULA} table.
 * Used to enrich PM data responses with formula, name, counters, and other details
 * for LLM consumption.
 */
@Repository
public class KpiFormulaRepository {

    private static final String TABLE = "KPI_FORMULA";

    /** Columns selected for enrichment (excludes FKs, timestamps, DELETED). */
    private static final String SELECT_COLUMNS =
            "KPI_CODE, KPI_NAME, KPI_UNIT, DESCRIPTION, KPI_FORMULA_DESC, KPI_FORMULA, "
            + "FORMULA_COUNTER_INFO, DOMAIN, VENDOR, TECHNOLOGY, AGGREGATION_LEVEL, "
            + "KPI_NODE_AGGREGATION, KPI_TIME_AGGREGATION, MO_TYPE, KPI_TYPE, ACCESS_TYPE, "
            + "VERSION, KPI_GROUP";

    private final JdbcTemplate jdbcTemplate;
    private final RowMapper<KpiFormulaDetails> rowMapper = this::mapRow;

    public KpiFormulaRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Finds KPI formula rows by code, only non-deleted records.
     * If multiple rows exist for the same KPI_CODE (e.g. different bands/vendors),
     * the first one returned by the database is kept in the map.
     *
     * @param kpiCodes KPI codes to look up (e.g. from Cassandra PM data)
     * @return map of KPI_CODE to details; empty if kpiCodes is empty or no matches
     */
    public Map<String, KpiFormulaDetails> findByKpiCodeIn(Collection<String> kpiCodes) {
        if (kpiCodes == null || kpiCodes.isEmpty()) {
            return Collections.emptyMap();
        }

        String inPlaceholders = String.join(",", Collections.nCopies(kpiCodes.size(), "?"));
        String sql = "SELECT " + SELECT_COLUMNS + " FROM " + TABLE
                + " WHERE KPI_CODE IN (" + inPlaceholders + ") AND (DELETED = 0 OR DELETED IS NULL)";

        java.util.List<KpiFormulaDetails> rows = jdbcTemplate.query(sql, kpiCodes.toArray(), rowMapper);
        Map<String, KpiFormulaDetails> result = new LinkedHashMap<>();
        for (KpiFormulaDetails details : rows) {
            String code = details.getKpiCode();
            if (code != null && !result.containsKey(code)) {
                result.put(code, details);
            }
        }
        return result;
    }

    private KpiFormulaDetails mapRow(ResultSet rs, int rowNum) throws SQLException {
        KpiFormulaDetails d = new KpiFormulaDetails();
        d.setKpiCode(rs.getString("KPI_CODE"));
        d.setKpiName(rs.getString("KPI_NAME"));
        d.setKpiUnit(rs.getString("KPI_UNIT"));
        d.setDescription(rs.getString("DESCRIPTION"));
        d.setFormulaCounterInfo(rs.getString("FORMULA_COUNTER_INFO"));
        d.setDomain(rs.getString("DOMAIN"));
        d.setVendor(rs.getString("VENDOR"));
        d.setTechnology(rs.getString("TECHNOLOGY"));
        d.setAggregationLevel(rs.getString("AGGREGATION_LEVEL"));
        d.setKpiNodeAggregation(rs.getString("KPI_NODE_AGGREGATION"));
        d.setKpiTimeAggregation(rs.getString("KPI_TIME_AGGREGATION"));
        d.setMoType(rs.getString("MO_TYPE"));
        d.setKpiType(rs.getString("KPI_TYPE"));
        d.setAccessType(rs.getString("ACCESS_TYPE"));
        d.setKpiGroup(rs.getString("KPI_GROUP"));
        return d;
    }
}
