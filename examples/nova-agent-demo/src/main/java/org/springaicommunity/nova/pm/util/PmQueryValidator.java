package org.springaicommunity.nova.pm.util;

import java.time.Instant;

import org.springaicommunity.nova.pm.dto.PmQueryRequest;
import org.springaicommunity.nova.pm.exception.PmValidationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Validates PM query parameters before they reach the repository layer.
 */
@Component
public class PmQueryValidator {

    @Value("${pm.query.max-result-size:10000}")
    private int maxResultSize;

    /**
     * Validates a POST query request.
     *
     * @throws PmValidationException if validation fails
     */
    public void validate(PmQueryRequest request) {
        if (request.getTimeRange().getFrom().isAfter(request.getTimeRange().getTo())) {
            throw new PmValidationException(
                    "timeRange.from must not be after timeRange.to");
        }
        if (request.getSize() <= 0) {
            throw new PmValidationException("page size must be greater than 0");
        }
        if (request.getSize() > maxResultSize) {
            throw new PmValidationException(
                    "page size must not exceed " + maxResultSize);
        }
    }

    /**
     * Validates individual GET endpoint parameters.
     *
     * <p>nodeName is checked here rather than via @NotBlank so we can return
     * a helpful message pointing callers to the multi-node POST endpoint.
     *
     * @throws PmValidationException if validation fails
     */
    public void validateGetParams(String domain, String vendor, String technology,
            String nodeName, Instant from, Instant to, int size) {
        if (nodeName == null || nodeName.isBlank()) {
            throw new PmValidationException(
                    "nodeName is required for single-node GET requests. " +
                    "To query all nodes, first discover available nodes via GET /pm/nodes " +
                    "then use POST /pm/data/query/enriched with the nodeNames list.");
        }
        if (from.isAfter(to)) {
            throw new PmValidationException("from must not be after to");
        }
        if (size <= 0 || size > maxResultSize) {
            throw new PmValidationException("size must be between 1 and " + maxResultSize);
        }
    }

}
