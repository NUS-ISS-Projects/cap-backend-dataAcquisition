package com.cap.dataAcquisition.controller;

import com.cap.dataAcquisition.model.EntityStateRecord;
import com.cap.dataAcquisition.model.FireEventRecord;
import com.cap.dataAcquisition.model.RealTimeMetrics; // Assuming this is still needed
import com.cap.dataAcquisition.model.AggregationResult; // New DTO
import com.cap.dataAcquisition.model.MonthlyAggregation; // New DTO
import com.cap.dataAcquisition.model.CustomRangeAggregation; // New DTO
import com.cap.dataAcquisition.repository.EntityStateRepository;
import com.cap.dataAcquisition.repository.FireEventRepository;
import com.cap.dataAcquisition.service.RealTimeMetricsService; // Assuming this is still needed
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.*; // For date-time manipulation
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
    
    // Assuming RealTimeMetricsService is for a different endpoint and still needed.
    // If you provided the dataAcquisition.txt without it, you can remove this.
    @Autowired(required = false) // Make it optional if it's not in all versions of your file
    private RealTimeMetricsService realTimeMetricsService;

    // Helper to convert epoch seconds to DIS Absolute Timestamp
    private long toDisAbsoluteTimestamp(long epochSeconds) {
        return epochSeconds | 0x80000000L;
    }

    // Helper to decode DIS Absolute Timestamp to epoch seconds
    private long fromDisAbsoluteTimestamp(long disTimestamp) {
        if ((disTimestamp & 0x80000000L) != 0) {
            return disTimestamp & 0x7FFFFFFFL;
        }
        // Handle or log if it's not an absolute timestamp or is zero
        return disTimestamp; // Or throw an error, or return a specific value for non-absolute
    }
    
    private String formatInstant(Instant instant) {
        return DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("UTC")).format(instant);
    }


    @GetMapping("/entity-states")
    public List<EntityStateRecord> getEntityStates(
            @RequestParam(required = false) Long startTime, // Expecting DIS Absolute Timestamp
            @RequestParam(required = false) Long endTime) { // Expecting DIS Absolute Timestamp
        List<EntityStateRecord> records;
        if (startTime != null && endTime != null) {
            log.info("Fetching entity states between DIS TS: {} ({}) and {} ({})", 
                startTime, formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(startTime))), 
                endTime, formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(endTime))));
            records = entityStateRepository.findByTimestampBetween(startTime, endTime);
        } else {
            log.info("Fetching all entity states.");
            records = entityStateRepository.findAll();
        }
        if (records != null && !records.isEmpty()) {
            log.info("Returning {} entity state records. First record raw DIS timestamp: {}, Decoded: {}",
                    records.size(),
                    records.get(0).getTimestamp(),
                    formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(records.get(0).getTimestamp()))));
        }
        return records;
    }

    @GetMapping("/fire-events")
    public List<FireEventRecord> getFireEvents(
            @RequestParam(required = false) Long startTime, // Expecting DIS Absolute Timestamp
            @RequestParam(required = false) Long endTime) { // Expecting DIS Absolute Timestamp
        List<FireEventRecord> records;
        if (startTime != null && endTime != null) {
             log.info("Fetching fire events between DIS TS: {} ({}) and {} ({})", 
                startTime, formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(startTime))), 
                endTime, formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(endTime))));
            records = fireEventRepository.findByTimestampBetween(startTime, endTime);
        } else {
            log.info("Fetching all fire events.");
            records = fireEventRepository.findAll();
        }
        if (records != null && !records.isEmpty()) {
            log.info("Returning {} fire event records. First record raw DIS timestamp: {}, Decoded: {}",
                    records.size(),
                    records.get(0).getTimestamp(),
                    formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(records.get(0).getTimestamp()))));
        }
        return records;
    }

    @GetMapping("/health")
    public ResponseEntity<String> healthCheck() {
        String podName = System.getenv("HOSTNAME");
        return ResponseEntity.ok("Data acquisition service is up and running on pod: " + podName);
    }

    // Only include if RealTimeMetricsService is configured and intended
    @GetMapping("/realtime")
    public ResponseEntity<RealTimeMetrics> getRealTimeDisMetrics() {
        if (realTimeMetricsService == null) {
            log.warn("/realtime endpoint called but RealTimeMetricsService is not available.");
            return ResponseEntity.status(503).body(new RealTimeMetrics(0,0,0.0)); // Service unavailable
        }
        RealTimeMetrics metrics = realTimeMetricsService.getLatestMetrics();
        if (metrics != null && metrics.getLastPduReceivedTimestampMs() > 0) {
            Instant instant = Instant.ofEpochMilli(metrics.getLastPduReceivedTimestampMs());
            log.debug("Realtime metrics: LastPduReceived at {}", formatInstant(instant));
        }
        return ResponseEntity.ok(metrics);
    }

    // --- NEW ENDPOINTS ---

    @GetMapping("/monthly")
    public ResponseEntity<MonthlyAggregation> getMonthlyAggregatedData(
            @RequestParam int year,
            @RequestParam int month) {

        if (month < 1 || month > 12) {
            return ResponseEntity.badRequest().body(null); // Or a proper error DTO
        }

        LocalDateTime startOfMonth = LocalDateTime.of(year, month, 1, 0, 0, 0);
        LocalDateTime endOfMonth = startOfMonth.plusMonths(1).minusNanos(1); // End of the last day of the month

        long startEpochSeconds = startOfMonth.atZone(ZoneId.of("UTC")).toEpochSecond();
        long endEpochSeconds = endOfMonth.atZone(ZoneId.of("UTC")).toEpochSecond();

        long disStartTime = toDisAbsoluteTimestamp(startEpochSeconds);
        long disEndTime = toDisAbsoluteTimestamp(endEpochSeconds);

        log.info("Fetching monthly aggregation for Year: {}, Month: {} (DIS TS Range: {} to {})", year, month, disStartTime, disEndTime);
        log.info("Corresponding UTC Range: {} to {}", formatInstant(Instant.ofEpochSecond(startEpochSeconds)), formatInstant(Instant.ofEpochSecond(endEpochSeconds)));


        List<EntityStateRecord> entityStates = entityStateRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<FireEventRecord> fireEvents = fireEventRepository.findByTimestampBetween(disStartTime, disEndTime);

        MonthlyAggregation result = new MonthlyAggregation(
                year,
                month,
                entityStates != null ? entityStates.size() : 0,
                fireEvents != null ? fireEvents.size() : 0
        );
        return ResponseEntity.ok(result);
    }

    @GetMapping("/aggregate")
    public ResponseEntity<CustomRangeAggregation> getCustomRangeAggregatedData(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate) {

        LocalDateTime startDateTime = startDate.atStartOfDay(); // Start of the start_date
        LocalDateTime endDateTime = endDate.atTime(LocalTime.MAX);   // End of the end_date

        long startEpochSeconds = startDateTime.atZone(ZoneId.of("UTC")).toEpochSecond();
        long endEpochSeconds = endDateTime.atZone(ZoneId.of("UTC")).toEpochSecond();
        
        long disStartTime = toDisAbsoluteTimestamp(startEpochSeconds);
        long disEndTime = toDisAbsoluteTimestamp(endEpochSeconds);

        log.info("Fetching custom range aggregation for Start: {}, End: {} (DIS TS Range: {} to {})", startDate, endDate, disStartTime, disEndTime);
        log.info("Corresponding UTC Range: {} to {}", formatInstant(Instant.ofEpochSecond(startEpochSeconds)), formatInstant(Instant.ofEpochSecond(endEpochSeconds)));

        List<EntityStateRecord> entityStates = entityStateRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<FireEventRecord> fireEvents = fireEventRepository.findByTimestampBetween(disStartTime, disEndTime);

        CustomRangeAggregation result = new CustomRangeAggregation(
                startDate.toString(),
                endDate.toString(),
                entityStates != null ? entityStates.size() : 0,
                fireEvents != null ? fireEvents.size() : 0
        );
        return ResponseEntity.ok(result);
    }
}