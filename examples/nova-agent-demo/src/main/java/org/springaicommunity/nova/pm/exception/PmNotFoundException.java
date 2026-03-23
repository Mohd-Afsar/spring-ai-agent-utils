package org.springaicommunity.nova.pm.exception;

/**
 * Thrown when a PM data query returns no results.
 */
public class PmNotFoundException extends PmException {

    public PmNotFoundException(String message) {
        super(message);
    }

}
