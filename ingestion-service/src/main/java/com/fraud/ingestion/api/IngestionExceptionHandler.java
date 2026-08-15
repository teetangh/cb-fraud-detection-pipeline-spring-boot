package com.fraud.ingestion.api;

import com.fraud.ingestion.infra.TransactionIngestor.IngestionFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

@RestControllerAdvice
public class IngestionExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(IngestionExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail onValidationFailure(MethodArgumentNotValidException e) {
        String detail = e.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + ": " + f.getDefaultMessage())
                .collect(Collectors.joining("; "));
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail);
        problem.setTitle("Invalid transaction");
        return problem;
    }

    /**
     * 503, not 500, and deliberately so.
     *
     * <p>This is the one place the system fails CLOSED. If the transaction cannot
     * be made durable then accepting it would be a promise we cannot keep — the
     * caller would believe it was recorded when it was not. A 503 tells the
     * caller to retry, which is both true and actionable.
     */
    @ExceptionHandler(IngestionFailedException.class)
    public ProblemDetail onIngestionFailure(IngestionFailedException e) {
        log.error("Ingestion failed — rejecting rather than accepting something we cannot "
                  + "make durable", e);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE,
                "Transaction could not be durably accepted; retry");
        problem.setTitle("Ingestion unavailable");
        return problem;
    }
}
