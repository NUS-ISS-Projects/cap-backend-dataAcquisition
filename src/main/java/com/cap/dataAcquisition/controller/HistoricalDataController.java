package com.cap.dataAcquisition.controller;

import com.cap.dataAcquisition.model.EntityStateRecord;
import com.cap.dataAcquisition.model.FireEventRecord;
import com.cap.dataAcquisition.model.RealTimeMetrics;
import com.cap.dataAcquisition.repository.EntityStateRepository;
import com.cap.dataAcquisition.repository.FireEventRepository;
import com.cap.dataAcquisition.service.RealTimeMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

@RestController
@RequestMapping("/api/acquisition")
public class HistoricalDataController {

    private static final Logger log = LoggerFactory.getLogger(HistoricalDataController.class);

    @Autowired
    private EntityStateRepository entityStateRepository; 
    @Autowired
    private FireEventRepository fireEventRepository; 
    @Autowired
    private RealTimeMetricsService realTimeMetricsService; 

    private String decodeDisTimestamp(long disTimestamp) {
        if (disTimestamp == 0) { 
            return "Timestamp is zero";
        }
        // Check if MSB is set (absolute time)
        if ((disTimestamp & 0x80000000L) != 0) {
            // Corrected: Removed underscore before L suffix
            long epochSeconds = disTimestamp & 0x7FFFFFFFL; // Clear the MSB to get epoch seconds 
            Instant instant = Instant.ofEpochSecond(epochSeconds);
            return DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("UTC")).format(instant) +
                   " (Epoch Seconds: " + epochSeconds + ")";
        } else {
            return "Relative DIS Timestamp or Unknown Format (Raw: " + disTimestamp + ")";
        }
    }

    @GetMapping("/entity-states")
    public List<EntityStateRecord> getEntityStates(
            @RequestParam(required = false) Long startTime,
            @RequestParam(required = false) Long endTime) {
        List<EntityStateRecord> records;
        if (startTime != null && endTime != null) {
            log.info("Fetching entity states between DIS TS: {} and {}", startTime, endTime);
             if (startTime > 1 && endTime > 1) { 
                 log.info("Decoded startTime: {} and endTime: {}", decodeDisTimestamp(startTime), decodeDisTimestamp(endTime));
            }
            records = entityStateRepository.findByTimestampBetween(startTime, endTime); //
        } else {
            log.info("Fetching all entity states.");
            records = entityStateRepository.findAll();
        }
        if (records != null && !records.isEmpty()) {
            log.info("Returning {} entity state records. First record raw DIS timestamp: {}, Decoded: {}",
                    records.size(),
                    records.get(0).getTimestamp(),
                    decodeDisTimestamp(records.get(0).getTimestamp()));
        }
        return records;
    }

    @GetMapping("/fire-events")
    public List<FireEventRecord> getFireEvents(
            @RequestParam(required = false) Long startTime,
            @RequestParam(required = false) Long endTime) {
        List<FireEventRecord> records;
        if (startTime != null && endTime != null) {
            log.info("Fetching fire events between DIS TS: {} and {}", startTime, endTime);
             if (startTime > 1 && endTime > 1) {
                 log.info("Decoded startTime: {} and endTime: {}", decodeDisTimestamp(startTime), decodeDisTimestamp(endTime));
            }
            records = fireEventRepository.findByTimestampBetween(startTime, endTime); //
        } else {
            log.info("Fetching all fire events.");
            records = fireEventRepository.findAll();
        }
        if (records != null && !records.isEmpty()) {
            log.info("Returning {} fire event records. First record raw DIS timestamp: {}, Decoded: {}",
                    records.size(),
                    records.get(0).getTimestamp(),
                    decodeDisTimestamp(records.get(0).getTimestamp()));
        }
        return records;
    }

    @GetMapping("/health")
    public ResponseEntity<String> healthCheck() {
        String podName = System.getenv("HOSTNAME");
        return ResponseEntity.ok("Data acquisition service is up and running on pod: " + podName); // [cite: 39]
    }

    @GetMapping("/realtime")
    public ResponseEntity<RealTimeMetrics> getRealTimeDisMetrics() {
        RealTimeMetrics metrics = realTimeMetricsService.getLatestMetrics(); 
        if (metrics != null && metrics.getLastPduReceivedTimestampMs() > 0) {
            Instant instant = Instant.ofEpochMilli(metrics.getLastPduReceivedTimestampMs());
            log.debug("Realtime metrics: LastPduReceived at {}", DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("UTC")).format(instant));
        }
        return ResponseEntity.ok(metrics); 
    }
}