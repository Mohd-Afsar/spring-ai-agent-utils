package org.springaicommunity.nova.pm.dto;

import java.time.Instant;

/**
 * Standardized error response for PM Data API errors.
 */
public class PmErrorResponse {

    private int status;
    private String error;
    private String message;
    private String path;
    private Instant timestamp;

    public PmErrorResponse() {
        this.timestamp = Instant.now();
    }

    public PmErrorResponse(int status, String error, String message, String path) {
        this();
        this.status = status;
        this.error = error;
        this.message = message;
        this.path = path;
    }

    public int getStatus() { return status; }
    public void setStatus(int status) { this.status = status; }

    public String getError() { return error; }
    public void setError(String error) { this.error = error; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getPath() { return path; }
    public void setPath(String path) { this.path = path; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

}
