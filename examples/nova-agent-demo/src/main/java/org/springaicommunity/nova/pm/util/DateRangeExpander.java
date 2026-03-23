package org.springaicommunity.nova.pm.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

/**
 * Expands a time range into a list of date strings (yyyyMMdd).
 *
 * <p>Cassandra PM tables are partitioned by date, so a query spanning multiple
 * days must issue one query per date partition. This utility computes all calendar
 * dates covered by a given time range.
 *
 * <p>Date format matches the actual Cassandra column value: {@code 20250623}.
 */
@Component
public class DateRangeExpander {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * Returns all calendar dates (inclusive) between from and to.
     *
     * @param from start of the time range (inclusive)
     * @param to   end of the time range (inclusive)
     * @return list of date strings in yyyyMMdd format (e.g. "20250623")
     */
    public List<String> expand(Instant from, Instant to) {
        LocalDate start = from.atZone(ZoneOffset.UTC).toLocalDate();
        LocalDate end = to.atZone(ZoneOffset.UTC).toLocalDate();

        List<String> dates = new ArrayList<>();
        LocalDate current = start;
        while (!current.isAfter(end)) {
            dates.add(current.format(DATE_FORMATTER));
            current = current.plusDays(1);
        }
        return dates;
    }

}
