package org.springaicommunity.nova.pm.exception;

import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springaicommunity.nova.pm.dto.PmErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Global exception handler for the PM Data Retrieval REST API.
 */
@RestControllerAdvice(basePackages = "org.springaicommunity.nova.pm")
public class PmExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(PmExceptionHandler.class);

    @ExceptionHandler(PmNotFoundException.class)
    public ResponseEntity<PmErrorResponse> handleNotFound(PmNotFoundException ex,
            HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new PmErrorResponse(404, "Not Found", ex.getMessage(), request.getRequestURI()));
    }

    @ExceptionHandler(PmValidationException.class)
    public ResponseEntity<PmErrorResponse> handleValidation(PmValidationException ex,
            HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new PmErrorResponse(400, "Bad Request", ex.getMessage(), request.getRequestURI()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<PmErrorResponse> handleBindValidation(MethodArgumentNotValidException ex,
            HttpServletRequest request) {
        String message = ex.getBindingResult().getFieldErrors().stream()
                .map(FieldError::getDefaultMessage)
                .collect(Collectors.joining("; "));
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new PmErrorResponse(400, "Validation Failed", message, request.getRequestURI()));
    }

    @ExceptionHandler(PmException.class)
    public ResponseEntity<PmErrorResponse> handleGeneral(PmException ex,
            HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new PmErrorResponse(500, "PM Data Error", ex.getMessage(), request.getRequestURI()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<PmErrorResponse> handleUnexpected(Exception ex,
            HttpServletRequest request) {
        log.error("Unhandled exception on {}", request.getRequestURI(), ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new PmErrorResponse(500, "Internal Server Error",
                        ex.getClass().getSimpleName() + ": " + ex.getMessage(),
                        request.getRequestURI()));
    }

}
