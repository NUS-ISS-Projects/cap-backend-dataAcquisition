package com.cap.dataAcquisition.controller;

import com.cap.dataAcquisition.model.CollisionRecord;
import com.cap.dataAcquisition.model.DetonationRecord;
import com.cap.dataAcquisition.model.EntityStateRecord;
import com.cap.dataAcquisition.model.FireEventRecord;
import com.cap.dataAcquisition.model.DataPduRecord;
import com.cap.dataAcquisition.model.ActionRequestPduRecord;
import com.cap.dataAcquisition.model.StartResumePduRecord;
import com.cap.dataAcquisition.model.SetDataPduRecord;
import com.cap.dataAcquisition.model.DesignatorPduRecord;
import com.cap.dataAcquisition.model.ElectromagneticEmissionsPduRecord;
import com.cap.dataAcquisition.model.RealTimeMetrics;
import com.cap.dataAcquisition.model.MonthlyAggregation;
import com.cap.dataAcquisition.model.CustomRangeAggregation;
import com.cap.dataAcquisition.model.AggregatedMetricsOverview;
import com.cap.dataAcquisition.dto.PduLogResponse;
import com.cap.dataAcquisition.repository.CollisionRepository;
import com.cap.dataAcquisition.repository.DetonationRepository;
import com.cap.dataAcquisition.repository.EntityStateRepository;
import com.cap.dataAcquisition.repository.FireEventRepository;
import com.cap.dataAcquisition.repository.DataPduRepository;
import com.cap.dataAcquisition.repository.ActionRequestPduRepository;
import com.cap.dataAcquisition.repository.StartResumePduRepository;
import com.cap.dataAcquisition.repository.SetDataPduRepository;
import com.cap.dataAcquisition.repository.DesignatorPduRepository;
import com.cap.dataAcquisition.repository.ElectromagneticEmissionsPduRepository;
import com.cap.dataAcquisition.service.MetricsService;
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

import java.time.DayOfWeek;
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
    private final CollisionRepository collisionRepository;
    private final DetonationRepository detonationRepository;
    private final DataPduRepository dataPduRepository;
    private final ActionRequestPduRepository actionRequestPduRepository;
    private final StartResumePduRepository startResumePduRepository;
    private final SetDataPduRepository setDataPduRepository;
    private final DesignatorPduRepository designatorPduRepository;
    private final ElectromagneticEmissionsPduRepository electromagneticEmissionsPduRepository;
    private final RealTimeMetricsService realTimeMetricsService;
    private final MetricsService metricsService;

    @Autowired
    public HistoricalDataController(EntityStateRepository entityStateRepository,
                                    FireEventRepository fireEventRepository,
                                    CollisionRepository collisionRepository,
                                    DetonationRepository detonationRepository,
                                    DataPduRepository dataPduRepository,
                                    ActionRequestPduRepository actionRequestPduRepository,
                                    StartResumePduRepository startResumePduRepository,
                                    SetDataPduRepository setDataPduRepository,
                                    DesignatorPduRepository designatorPduRepository,
                                    ElectromagneticEmissionsPduRepository electromagneticEmissionsPduRepository,
                                    @Autowired(required = false) RealTimeMetricsService realTimeMetricsService,
                                    MetricsService metricsService) {
        this.entityStateRepository = entityStateRepository;
        this.fireEventRepository = fireEventRepository;
        this.collisionRepository = collisionRepository;
        this.detonationRepository = detonationRepository;
        this.dataPduRepository = dataPduRepository;
        this.actionRequestPduRepository = actionRequestPduRepository;
        this.startResumePduRepository = startResumePduRepository;
        this.setDataPduRepository = setDataPduRepository;
        this.designatorPduRepository = designatorPduRepository;
        this.electromagneticEmissionsPduRepository = electromagneticEmissionsPduRepository;
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

    @GetMapping("/collision-events")
    public List<CollisionRecord> getCollisionEvents(
            @RequestParam(required = false) Long startTime, // Expecting DIS Absolute Timestamp
            @RequestParam(required = false) Long endTime) { // Expecting DIS Absolute Timestamp
        List<CollisionRecord> records;
        if (startTime != null && endTime != null) {
             log.info("Fetching collision events between DIS TS: {} ({}) and {} ({})",
                startTime, MetricsService.formatInstant(Instant.ofEpochSecond(MetricsService.fromDisAbsoluteTimestamp(startTime))),
                endTime, MetricsService.formatInstant(Instant.ofEpochSecond(MetricsService.fromDisAbsoluteTimestamp(endTime))));
            records = collisionRepository.findByTimestampBetween(startTime, endTime);
        } else {
            log.info("Fetching all collision events.");
            records = collisionRepository.findAll();
        }
        if (records != null && !records.isEmpty()) {
            log.info("Returning {} collision event records. First record raw DIS timestamp: {}, Decoded: {}",
                    records.size(),
                    records.get(0).getTimestamp(),
                    MetricsService.formatInstant(Instant.ofEpochSecond(MetricsService.fromDisAbsoluteTimestamp(records.get(0).getTimestamp()))));
        }
        return records;
    }

    @GetMapping("/detonation-events")
    public List<DetonationRecord> getDetonationEvents(
            @RequestParam(required = false) Long startTime, // Expecting DIS Absolute Timestamp
            @RequestParam(required = false) Long endTime) { // Expecting DIS Absolute Timestamp
        List<DetonationRecord> records;
        if (startTime != null && endTime != null) {
             log.info("Fetching detonation events between DIS TS: {} ({}) and {} ({})",
                startTime, MetricsService.formatInstant(Instant.ofEpochSecond(MetricsService.fromDisAbsoluteTimestamp(startTime))),
                endTime, MetricsService.formatInstant(Instant.ofEpochSecond(MetricsService.fromDisAbsoluteTimestamp(endTime))));
            records = detonationRepository.findByTimestampBetween(startTime, endTime);
        } else {
            log.info("Fetching all detonation events.");
            records = detonationRepository.findAll();
        }
        if (records != null && !records.isEmpty()) {
            log.info("Returning {} detonation event records. First record raw DIS timestamp: {}, Decoded: {}",
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
            return ResponseEntity.status(503).body(new RealTimeMetrics(0,0,0.0,0,0,0,0)); // Service unavailable
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
        List<CollisionRecord> collisionEvents = collisionRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<DetonationRecord> detonationEvents = detonationRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<DataPduRecord> dataPduEvents = dataPduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<ActionRequestPduRecord> actionRequestPduEvents = actionRequestPduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<StartResumePduRecord> startResumePduEvents = startResumePduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<SetDataPduRecord> setDataPduEvents = setDataPduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<DesignatorPduRecord> designatorPduEvents = designatorPduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<ElectromagneticEmissionsPduRecord> electromagneticEmissionsPduEvents = electromagneticEmissionsPduRepository.findByTimestampBetween(disStartTime, disEndTime);

        MonthlyAggregation result = new MonthlyAggregation(
                year,
                month,
                entityStates != null ? entityStates.size() : 0,
                fireEvents != null ? fireEvents.size() : 0,
                collisionEvents != null ? collisionEvents.size() : 0,
                detonationEvents != null ? detonationEvents.size() : 0,
                dataPduEvents != null ? dataPduEvents.size() : 0,
                actionRequestPduEvents != null ? actionRequestPduEvents.size() : 0,
                startResumePduEvents != null ? startResumePduEvents.size() : 0,
                setDataPduEvents != null ? setDataPduEvents.size() : 0,
                designatorPduEvents != null ? designatorPduEvents.size() : 0,
                electromagneticEmissionsPduEvents != null ? electromagneticEmissionsPduEvents.size() : 0
        );
        return ResponseEntity.ok(result);
    }

    @GetMapping("/aggregate")
    public ResponseEntity<CustomRangeAggregation> getCustomRangeAggregatedData(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false, defaultValue = "false") boolean today,
            @RequestParam(required = false, defaultValue = "false") boolean week,
            @RequestParam(required = false, defaultValue = "false") boolean month) {

        LocalDate actualStartDate;
        LocalDate actualEndDate;
        
        try {
            if (today) {
                // Show all data for today
                actualStartDate = LocalDate.now(ZoneId.of("UTC"));
                actualEndDate = actualStartDate;
                log.info("Fetching aggregation for today: {}", actualStartDate);
            } else if (startDate != null && week) {
                // Show data for the given date and the past week (Monday is start of week)
                LocalDate parsedStartDate = LocalDate.parse(startDate);
                LocalDate mondayOfWeek = parsedStartDate.with(DayOfWeek.MONDAY);
                actualStartDate = mondayOfWeek;
                actualEndDate = parsedStartDate;
                log.info("Fetching aggregation for week from {} to {}", actualStartDate, actualEndDate);
            } else if (startDate != null && month) {
                // Show data for the given date and the past month
                LocalDate parsedStartDate = LocalDate.parse(startDate);
                LocalDate firstOfMonth = parsedStartDate.withDayOfMonth(1);
                actualStartDate = firstOfMonth;
                actualEndDate = parsedStartDate;
                log.info("Fetching aggregation for month from {} to {}", actualStartDate, actualEndDate);
            } else if (startDate != null && endDate != null) {
                // Custom range
                actualStartDate = LocalDate.parse(startDate);
                actualEndDate = LocalDate.parse(endDate);
                log.info("Fetching aggregation for custom range from {} to {}", actualStartDate, actualEndDate);
            } else if (startDate != null) {
                // Only date selected: show all data within the given date
                actualStartDate = LocalDate.parse(startDate);
                actualEndDate = actualStartDate;
                log.info("Fetching aggregation for single date: {}", actualStartDate);
            } else {
                return ResponseEntity.badRequest().body(null);
            }
        } catch (Exception e) {
            log.error("Error parsing date parameters: {}", e.getMessage());
            return ResponseEntity.badRequest().body(null);
        }

        LocalDateTime startDateTime = actualStartDate.atStartOfDay();
        LocalDateTime endDateTime = actualEndDate.atTime(LocalTime.MAX);

        long startEpochSeconds = startDateTime.atZone(ZoneId.of("UTC")).toEpochSecond();
        long endEpochSeconds = endDateTime.atZone(ZoneId.of("UTC")).toEpochSecond();

        long disStartTime = MetricsService.toDisAbsoluteTimestamp(startEpochSeconds);
        long disEndTime = MetricsService.toDisAbsoluteTimestamp(endEpochSeconds);

        log.info("DIS TS Range: {} to {}", disStartTime, disEndTime);
        log.info("Corresponding UTC Range: {} to {}", MetricsService.formatInstant(Instant.ofEpochSecond(startEpochSeconds)), MetricsService.formatInstant(Instant.ofEpochSecond(endEpochSeconds)));

        List<EntityStateRecord> entityStates = entityStateRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<FireEventRecord> fireEvents = fireEventRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<CollisionRecord> collisionEvents = collisionRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<DetonationRecord> detonationEvents = detonationRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<DataPduRecord> dataPduEvents = dataPduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<ActionRequestPduRecord> actionRequestPduEvents = actionRequestPduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<StartResumePduRecord> startResumePduEvents = startResumePduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<SetDataPduRecord> setDataPduEvents = setDataPduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<DesignatorPduRecord> designatorPduEvents = designatorPduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<ElectromagneticEmissionsPduRecord> electromagneticEmissionsPduEvents = electromagneticEmissionsPduRepository.findByTimestampBetween(disStartTime, disEndTime);

        CustomRangeAggregation result = new CustomRangeAggregation(
                actualStartDate.toString(),
                actualEndDate.toString(),
                entityStates != null ? entityStates.size() : 0,
                fireEvents != null ? fireEvents.size() : 0,
                collisionEvents != null ? collisionEvents.size() : 0,
                detonationEvents != null ? detonationEvents.size() : 0,
                dataPduEvents != null ? dataPduEvents.size() : 0,
                actionRequestPduEvents != null ? actionRequestPduEvents.size() : 0,
                startResumePduEvents != null ? startResumePduEvents.size() : 0,
                setDataPduEvents != null ? setDataPduEvents.size() : 0,
                designatorPduEvents != null ? designatorPduEvents.size() : 0,
                electromagneticEmissionsPduEvents != null ? electromagneticEmissionsPduEvents.size() : 0
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

    // --- NEW REALTIME LOGS ENDPOINT ---
    @GetMapping("/realtime/logs")
    public ResponseEntity<PduLogResponse> getRealtimePduLogs(
            @RequestParam Long startTime, // Expecting Unix Epoch Timestamp (seconds)
            @RequestParam Long endTime) { // Expecting Unix Epoch Timestamp (seconds)
        
        log.info("Fetching realtime PDU logs between Unix Epoch: {} ({}) and {} ({})",
            startTime, MetricsService.formatInstant(Instant.ofEpochSecond(startTime)),
            endTime, MetricsService.formatInstant(Instant.ofEpochSecond(endTime)));
        
        // Convert Unix epoch timestamps to DIS timestamps for internal processing
        Long disStartTime = MetricsService.toDisAbsoluteTimestamp(startTime);
        Long disEndTime = MetricsService.toDisAbsoluteTimestamp(endTime);
        
        PduLogResponse response = metricsService.getAllPduLogs(disStartTime, disEndTime);
        return ResponseEntity.ok(response);
    }
}