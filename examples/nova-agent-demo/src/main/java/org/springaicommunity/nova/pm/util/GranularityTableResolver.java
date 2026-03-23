package org.springaicommunity.nova.pm.util;

import org.springaicommunity.nova.pm.exception.PmValidationException;
import org.springaicommunity.nova.pm.model.Granularity;
import org.springframework.stereotype.Component;

/**
 * Resolves the correct Cassandra table name for a given granularity.
 *
 * <p>Centralizing this mapping makes it easy to add new granularity levels
 * in the future without touching query logic.
 */
@Component
public class GranularityTableResolver {

    /**
     * Returns the Cassandra table name for the given granularity.
     *
     * @param granularity the requested data granularity
     * @return the Cassandra table name
     * @throws PmValidationException if granularity is null
     */
    public String resolve(Granularity granularity) {
        if (granularity == null) {
            throw new PmValidationException("Granularity must not be null");
        }
        return granularity.getTableName();
    }

}
