package org.springaicommunity.nova.pm.model;

/**
 * Granularity levels for PM KPI data.
 * Each value maps directly to its pre-aggregated Cassandra table.
 */
public enum Granularity {

    FIVE_MIN("combine5minpm"),
    QUARTERLY("combinequarterlypm"),
    HOURLY("combinehourlypm"),
    DAILY("combinedailypm"),
    WEEKLY("combineweeklypm"),
    MONTHLY("combinemonthlypm");

    private final String tableName;

    Granularity(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

}
