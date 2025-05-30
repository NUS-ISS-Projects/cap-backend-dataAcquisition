package com.cap.dataAcquisition.controller;

import com.cap.dataAcquisition.model.EntityStateRecord;
import com.cap.dataAcquisition.model.FireEventRecord;
import com.cap.dataAcquisition.model.RealTimeMetrics;
import com.cap.dataAcquisition.model.MonthlyAggregation;
import com.cap.dataAcquisition.model.CustomRangeAggregation;
import com.cap.dataAcquisition.model.AggregatedMetricsOverview;
import com.cap.dataAcquisition.repository.EntityStateRepository;
import com.cap.dataAcquisition.repository.FireEventRepository;
import com.cap.dataAcquisition.service.MetricsService; // Import the new service
import com.cap.dataAcquisition.service.RealTimeMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId; // Keep ZoneId if used by original methods
import java.util.List;

@RestController
@RequestMapping("/api/acquisition")
public class HistoricalDataController {

    private static final Logger log = LoggerFactory.getLogger(HistoricalDataController.class);

    private final EntityStateRepository entityStateRepository;
    private final FireEventRepository fireEventRepository;
    private final RealTimeMetricsService realTimeMetricsService;
    private final MetricsService metricsService; // Inject new service

    @Autowired
    public HistoricalDataController(EntityStateRepository entityStateRepository,
                                    FireEventRepository fireEventRepository,
                                    @Autowired(required = false) RealTimeMetricsService realTimeMetricsService,
                                    MetricsService metricsService) {
        this.entityStateRepository = entityStateRepository;
        this.fireEventRepository = fireEventRepository;
        this.realTimeMetricsService = realTimeMetricsService;
        this.metricsService = metricsService;
    }

    @GetMapping("/entity-states")
    public List<EntityStateRecord> getEntityStates(
            @RequestParam(required = false) Long startTime, // Expecting DIS Absolute Timestamp
            @RequestParam(required = false) Long endTime) { // Expecting DIS Absolute Timestamp
        List<EntityStateRecord> records;
        if (startTime != null && endTime != null) {
            log.info("Fetching entity states between DIS TS: {} ({}) and {} ({})",
                startTime, MetricsService.formatInstant(Instant.ofEpochSecond(MetricsService.fromDisAbsoluteTimestamp(startTime))),
                endTime, MetricsService.formatInstant(Instant.ofEpochSecond(MetricsService.fromDisAbsoluteTimestamp(endTime))));
            records = entityStateRepository.findByTimestampBetween(startTime, endTime);
        } else {
            log.info("Fetching all entity states.");
            records = entityStateRepository.findAll();
        }
        if (records != null && !records.isEmpty()) {
            log.info("Returning {} entity state records. First record raw DIS timestamp: {}, Decoded: {}",
                    records.size(),
                    records.get(0).getTimestamp(),
                    MetricsService.formatInstant(Instant.ofEpochSecond(MetricsService.fromDisAbsoluteTimestamp(records.get(0).getTimestamp()))));
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
                startTime, MetricsService.formatInstant(Instant.ofEpochSecond(MetricsService.fromDisAbsoluteTimestamp(startTime))),
                endTime, MetricsService.formatInstant(Instant.ofEpochSecond(MetricsService.fromDisAbsoluteTimestamp(endTime))));
            records = fireEventRepository.findByTimestampBetween(startTime, endTime);
        } else {
            log.info("Fetching all fire events.");
            records = fireEventRepository.findAll();
        }
        if (records != null && !records.isEmpty()) {
            log.info("Returning {} fire event records. First record raw DIS timestamp: {}, Decoded: {}",
                    records.size(),
                    records.get(0).getTimestamp(),
                    MetricsService.formatInstant(Instant.ofEpochSecond(MetricsService.fromDisAbsoluteTimestamp(records.get(0).getTimestamp()))));
        }
        return records;
    }

    @GetMapping("/health")
    public ResponseEntity<String> healthCheck() {
        String podName = System.getenv("HOSTNAME");
        return ResponseEntity.ok("Data acquisition service is up and running on pod: " + podName);
    }

    @GetMapping("/realtime")
    public ResponseEntity<RealTimeMetrics> getRealTimeDisMetrics() {
        if (realTimeMetricsService == null) {
            log.warn("/realtime endpoint called but RealTimeMetricsService is not available.");
            return ResponseEntity.status(503).body(new RealTimeMetrics(0,0,0.0)); // Service unavailable
        }
        RealTimeMetrics metrics = realTimeMetricsService.getLatestMetrics();
        if (metrics != null && metrics.getLastPduReceivedTimestampMs() > 0) {
            Instant instant = Instant.ofEpochMilli(metrics.getLastPduReceivedTimestampMs());
            log.debug("Realtime metrics: LastPduReceived at {}", MetricsService.formatInstant(instant));
        }
        return ResponseEntity.ok(metrics);
    }

    @GetMapping("/monthly")
    public ResponseEntity<MonthlyAggregation> getMonthlyAggregatedData(
            @RequestParam int year,
            @RequestParam int month) {

        if (month < 1 || month > 12) {
            return ResponseEntity.badRequest().body(null);
        }

        LocalDateTime startOfMonth = LocalDateTime.of(year, month, 1, 0, 0, 0);
        LocalDateTime endOfMonth = startOfMonth.plusMonths(1).minusNanos(1);

        long startEpochSeconds = startOfMonth.atZone(ZoneId.of("UTC")).toEpochSecond();
        long endEpochSeconds = endOfMonth.atZone(ZoneId.of("UTC")).toEpochSecond();

        long disStartTime = MetricsService.toDisAbsoluteTimestamp(startEpochSeconds);
        long disEndTime = MetricsService.toDisAbsoluteTimestamp(endEpochSeconds);

        log.info("Fetching monthly aggregation for Year: {}, Month: {} (DIS TS Range: {} to {})", year, month, disStartTime, disEndTime);
        log.info("Corresponding UTC Range: {} to {}", MetricsService.formatInstant(Instant.ofEpochSecond(startEpochSeconds)), MetricsService.formatInstant(Instant.ofEpochSecond(endEpochSeconds)));

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

        LocalDateTime startDateTime = startDate.atStartOfDay();
        LocalDateTime endDateTime = endDate.atTime(LocalTime.MAX);

        long startEpochSeconds = startDateTime.atZone(ZoneId.of("UTC")).toEpochSecond();
        long endEpochSeconds = endDateTime.atZone(ZoneId.of("UTC")).toEpochSecond();

        long disStartTime = MetricsService.toDisAbsoluteTimestamp(startEpochSeconds);
        long disEndTime = MetricsService.toDisAbsoluteTimestamp(endEpochSeconds);

        log.info("Fetching custom range aggregation for Start: {}, End: {} (DIS TS Range: {} to {})", startDate, endDate, disStartTime, disEndTime);
        log.info("Corresponding UTC Range: {} to {}", MetricsService.formatInstant(Instant.ofEpochSecond(startEpochSeconds)), MetricsService.formatInstant(Instant.ofEpochSecond(endEpochSeconds)));

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

    // --- NEW METRICS ENDPOINT using MetricsService ---
    @GetMapping("/metrics")
    public ResponseEntity<AggregatedMetricsOverview> getAggregatedMetricsOverview(
            @RequestParam(required = false, defaultValue = "last60minutes") String period) {
        
        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics(period);
        return ResponseEntity.ok(overview);
    }
}