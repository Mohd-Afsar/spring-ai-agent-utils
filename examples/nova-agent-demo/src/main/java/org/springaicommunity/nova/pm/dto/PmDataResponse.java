package org.springaicommunity.nova.pm.dto;

import java.util.List;

import org.springaicommunity.nova.pm.model.Granularity;

/**
 * Response returned by the PM Data Retrieval API.
 * Structured for direct consumption by AI agents (NOVA sub-agents).
 */
public class PmDataResponse {

    private String domain;
    private String vendor;
    private String technology;
    private Granularity granularity;
    private String dataLevel;
    private String nodeName;
    private TimeRange timeRange;

    private List<PmDataPoint> data;

    /** Total number of data points returned (across all pages if paginated). */
    private long totalPoints;

    /** Current page number (0-based). */
    private int page;

    /** Page size used. */
    private int pageSize;

    /** True if more pages are available. */
    private boolean hasMore;

    public PmDataResponse() {
    }

    public String getDomain() { return domain; }
    public void setDomain(String domain) { this.domain = domain; }

    public String getVendor() { return vendor; }
    public void setVendor(String vendor) { this.vendor = vendor; }

    public String getTechnology() { return technology; }
    public void setTechnology(String technology) { this.technology = technology; }

    public Granularity getGranularity() { return granularity; }
    public void setGranularity(Granularity granularity) { this.granularity = granularity; }

    public String getDataLevel() { return dataLevel; }
    public void setDataLevel(String dataLevel) { this.dataLevel = dataLevel; }

    public String getNodeName() { return nodeName; }
    public void setNodeName(String nodeName) { this.nodeName = nodeName; }

    public TimeRange getTimeRange() { return timeRange; }
    public void setTimeRange(TimeRange timeRange) { this.timeRange = timeRange; }

    public List<PmDataPoint> getData() { return data; }
    public void setData(List<PmDataPoint> data) { this.data = data; }

    public long getTotalPoints() { return totalPoints; }
    public void setTotalPoints(long totalPoints) { this.totalPoints = totalPoints; }

    public int getPage() { return page; }
    public void setPage(int page) { this.page = page; }

    public int getPageSize() { return pageSize; }
    public void setPageSize(int pageSize) { this.pageSize = pageSize; }

    public boolean isHasMore() { return hasMore; }
    public void setHasMore(boolean hasMore) { this.hasMore = hasMore; }

}
