package com.fraud.ingestion.api;

import com.fraud.ingestion.domain.IngestionResult;
import com.fraud.ingestion.domain.TransactionRecord;
import com.fraud.ingestion.infra.TransactionIngestor;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/internal/v1")
public class IngestionController {

    private static final Logger log = LoggerFactory.getLogger(IngestionController.class);

    private final TransactionIngestor ingestor;

    public IngestionController(TransactionIngestor ingestor) {
        this.ingestor = ingestor;
    }

    /**
     * Internal endpoint — called by gateway-service, never exposed externally.
     *
     * <p>Returns 202 once the transaction is durably committed, which is the point
     * after which loss is impossible. The pipeline has not run yet and no
     * decision exists; the gateway is waiting on Redis Pub/Sub for that.
     *
     * <p>A duplicate returns 202 with the SAME payload the original caller got —
     * not a 409. The client did nothing wrong: retrying after an ambiguous
     * network timeout is correct behaviour, and the correct answer is the
     * original answer (§9.1).
     */
    @PostMapping("/ingest")
    public ResponseEntity<IngestionResult> ingest(@Valid @RequestBody TransactionRecord txn) {
        IngestionResult result = ingestor.ingest(txn);
        return ResponseEntity.accepted().body(result);
    }
}
