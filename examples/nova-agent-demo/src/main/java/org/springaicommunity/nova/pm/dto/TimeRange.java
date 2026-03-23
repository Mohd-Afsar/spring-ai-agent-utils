package org.springaicommunity.nova.pm.dto;

import java.time.Instant;

import jakarta.validation.constraints.NotNull;

/**
 * Time range for a PM data query.
 */
public class TimeRange {

    @NotNull(message = "from timestamp is required")
    private Instant from;

    @NotNull(message = "to timestamp is required")
    private Instant to;

    public TimeRange() {
    }

    public TimeRange(Instant from, Instant to) {
        this.from = from;
        this.to = to;
    }

    public Instant getFrom() { return from; }
    public void setFrom(Instant from) { this.from = from; }

    public Instant getTo() { return to; }
    public void setTo(Instant to) { this.to = to; }

}
