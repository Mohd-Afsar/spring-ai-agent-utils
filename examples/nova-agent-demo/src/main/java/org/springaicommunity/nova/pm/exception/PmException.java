package org.springaicommunity.nova.pm.exception;

/**
 * Base exception for the PM Data Retrieval module.
 */
public class PmException extends RuntimeException {

    public PmException(String message) {
        super(message);
    }

    public PmException(String message, Throwable cause) {
        super(message, cause);
    }

}
