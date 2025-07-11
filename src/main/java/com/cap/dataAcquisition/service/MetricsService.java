package com.cap.dataAcquisition.service;

import com.cap.dataAcquisition.model.AggregatedMetricsOverview;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class MetricsService {

    private static final Logger log = LoggerFactory.getLogger(MetricsService.class);

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

    @Autowired
    public MetricsService(EntityStateRepository entityStateRepository, 
                          FireEventRepository fireEventRepository,
                          CollisionRepository collisionRepository,
                          DetonationRepository detonationRepository,
                          DataPduRepository dataPduRepository,
                          ActionRequestPduRepository actionRequestPduRepository,
                          StartResumePduRepository startResumePduRepository,
                          SetDataPduRepository setDataPduRepository,
                          DesignatorPduRepository designatorPduRepository,
                          ElectromagneticEmissionsPduRepository electromagneticEmissionsPduRepository) {
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
    }

    // --- Public Static Helper Methods for Timestamp Conversion & Formatting ---
    public static long toDisAbsoluteTimestamp(long epochSeconds) {
        return epochSeconds | 0x80000000L;
    }

    public static long fromDisAbsoluteTimestamp(long disTimestamp) {
        if ((disTimestamp & 0x80000000L) != 0) {
            return disTimestamp & 0x7FFFFFFFL;
        }
        log.warn("Timestamp {} does not have the MSB set, returning as is. It might not be a DIS absolute timestamp.", disTimestamp);
        return disTimestamp;
    }

    public static String formatInstant(Instant instant) {
        if (instant == null) return "N/A";
        return DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("UTC")).format(instant);
    }

    // --- Core Service Logic for Aggregated Metrics ---
    public AggregatedMetricsOverview getAggregatedMetrics(String period) {
        Instant endTimeUtc = Instant.now();
        Instant startTimeUtc;
        String timeWindowDescription;

        if ("last60minutes".equalsIgnoreCase(period)) {
            startTimeUtc = endTimeUtc.minus(60, ChronoUnit.MINUTES);
            timeWindowDescription = "Last 60 minutes";
        } else if ("lastDay".equalsIgnoreCase(period)) { // Added example for "lastDay"
            startTimeUtc = endTimeUtc.minus(1, ChronoUnit.DAYS);
            timeWindowDescription = "Last 24 hours";
        } else {
            startTimeUtc = endTimeUtc.minus(60, ChronoUnit.MINUTES); // Default
            timeWindowDescription = "Last 60 minutes (default)";
            log.warn("Unsupported period requested for metrics: '{}'. Defaulting to last 60 minutes.", period);
        }

        long startEpochSeconds = startTimeUtc.getEpochSecond();
        long endEpochSeconds = endTimeUtc.getEpochSecond();

        long disStartTime = toDisAbsoluteTimestamp(startEpochSeconds);
        long disEndTime = toDisAbsoluteTimestamp(endEpochSeconds);

        log.info("Fetching aggregated metrics for period: {} (DIS TS Range: {} to {})",
                timeWindowDescription, disStartTime, disEndTime);
        log.info("Corresponding UTC Range: {} to {}",
                formatInstant(startTimeUtc), formatInstant(endTimeUtc));

        List<EntityStateRecord> entityStates = entityStateRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<FireEventRecord> fireEvents = fireEventRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<CollisionRecord> collisionEvents = collisionRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<DetonationRecord> detonationEvents = detonationRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<DataPduRecord> dataPduEvents = dataPduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<ActionRequestPduRecord> actionRequestEvents = actionRequestPduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<StartResumePduRecord> startResumeEvents = startResumePduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<SetDataPduRecord> setDataEvents = setDataPduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<DesignatorPduRecord> designatorEvents = designatorPduRepository.findByTimestampBetween(disStartTime, disEndTime);
        List<ElectromagneticEmissionsPduRecord> electromagneticEmissionsEvents = electromagneticEmissionsPduRepository.findByTimestampBetween(disStartTime, disEndTime);

        long totalEntityStatePackets = entityStates != null ? entityStates.size() : 0;
        long totalFireEventPackets = fireEvents != null ? fireEvents.size() : 0;
        long totalCollisionPackets = collisionEvents != null ? collisionEvents.size() : 0;
        long totalDetonationPackets = detonationEvents != null ? detonationEvents.size() : 0;
        long totalDataPduPackets = dataPduEvents != null ? dataPduEvents.size() : 0;
        long totalActionRequestPackets = actionRequestEvents != null ? actionRequestEvents.size() : 0;
        long totalStartResumePackets = startResumeEvents != null ? startResumeEvents.size() : 0;
        long totalSetDataPackets = setDataEvents != null ? setDataEvents.size() : 0;
        long totalDesignatorPackets = designatorEvents != null ? designatorEvents.size() : 0;
        long totalElectromagneticEmissionsPackets = electromagneticEmissionsEvents != null ? electromagneticEmissionsEvents.size() : 0;
        long totalPackets = totalEntityStatePackets + totalFireEventPackets + totalCollisionPackets + totalDetonationPackets + 
                           totalDataPduPackets + totalActionRequestPackets + totalStartResumePackets + totalSetDataPackets + 
                           totalDesignatorPackets + totalElectromagneticEmissionsPackets;

        double durationSeconds = ChronoUnit.SECONDS.between(startTimeUtc, endTimeUtc);
        double averagePacketsPerSecond = (durationSeconds > 0) ? (totalPackets / durationSeconds) : 0.0;

        AggregatedMetricsOverview.PeakLoadInfo peakLoadInfo = calculatePeakLoad(
                entityStates, fireEvents, collisionEvents, detonationEvents, 
                dataPduEvents, actionRequestEvents, startResumeEvents, setDataEvents, 
                designatorEvents, electromagneticEmissionsEvents, startTimeUtc, endTimeUtc
        );

        return new AggregatedMetricsOverview(
                timeWindowDescription,
                startTimeUtc,
                endTimeUtc,
                totalPackets,
                totalEntityStatePackets,
                totalFireEventPackets,
                totalCollisionPackets,
                totalDetonationPackets,
                totalDataPduPackets,
                totalActionRequestPackets,
                totalStartResumePackets,
                totalSetDataPackets,
                totalDesignatorPackets,
                totalElectromagneticEmissionsPackets,
                averagePacketsPerSecond,
                peakLoadInfo
        );
    }

    private AggregatedMetricsOverview.PeakLoadInfo calculatePeakLoad(
            List<EntityStateRecord> entityStates,
            List<FireEventRecord> fireEvents,
            List<CollisionRecord> collisionEvents,
            List<DetonationRecord> detonationEvents,
            List<DataPduRecord> dataPduEvents,
            List<ActionRequestPduRecord> actionRequestEvents,
            List<StartResumePduRecord> startResumeEvents,
            List<SetDataPduRecord> setDataEvents,
            List<DesignatorPduRecord> designatorEvents,
            List<ElectromagneticEmissionsPduRecord> electromagneticEmissionsEvents,
            Instant windowStartTimeUtc,
            Instant windowEndTimeUtc) {

        // Explicit typing in map can help with "capture#1-of ? extends Object" issues if Lombok/IDE struggles
        Stream<Long> entityTimestamps = (entityStates != null ? entityStates.stream() : Stream.<EntityStateRecord>empty())
                .map((EntityStateRecord record) -> fromDisAbsoluteTimestamp(record.getTimestamp()));
        Stream<Long> fireTimestamps = (fireEvents != null ? fireEvents.stream() : Stream.<FireEventRecord>empty())
                .map((FireEventRecord record) -> fromDisAbsoluteTimestamp(record.getTimestamp()));
        Stream<Long> collisionTimestamps = (collisionEvents != null ? collisionEvents.stream() : Stream.<CollisionRecord>empty())
                .map((CollisionRecord record) -> fromDisAbsoluteTimestamp(record.getTimestamp()));
        Stream<Long> detonationTimestamps = (detonationEvents != null ? detonationEvents.stream() : Stream.<DetonationRecord>empty())
                .map((DetonationRecord record) -> fromDisAbsoluteTimestamp(record.getTimestamp()));
        Stream<Long> dataPduTimestamps = (dataPduEvents != null ? dataPduEvents.stream() : Stream.<DataPduRecord>empty())
                .map((DataPduRecord record) -> fromDisAbsoluteTimestamp(record.getTimestamp()));
        Stream<Long> actionRequestTimestamps = (actionRequestEvents != null ? actionRequestEvents.stream() : Stream.<ActionRequestPduRecord>empty())
                .map((ActionRequestPduRecord record) -> fromDisAbsoluteTimestamp(record.getTimestamp()));
        Stream<Long> startResumeTimestamps = (startResumeEvents != null ? startResumeEvents.stream() : Stream.<StartResumePduRecord>empty())
                .map((StartResumePduRecord record) -> fromDisAbsoluteTimestamp(record.getTimestamp()));
        Stream<Long> setDataTimestamps = (setDataEvents != null ? setDataEvents.stream() : Stream.<SetDataPduRecord>empty())
                .map((SetDataPduRecord record) -> fromDisAbsoluteTimestamp(record.getTimestamp()));
        Stream<Long> designatorTimestamps = (designatorEvents != null ? designatorEvents.stream() : Stream.<DesignatorPduRecord>empty())
                .map((DesignatorPduRecord record) -> fromDisAbsoluteTimestamp(record.getTimestamp()));
        Stream<Long> electromagneticEmissionsTimestamps = (electromagneticEmissionsEvents != null ? electromagneticEmissionsEvents.stream() : Stream.<ElectromagneticEmissionsPduRecord>empty())
                .map((ElectromagneticEmissionsPduRecord record) -> fromDisAbsoluteTimestamp(record.getTimestamp()));

        List<Long> allPduEpochSeconds = Stream.of(
                entityTimestamps, fireTimestamps, collisionTimestamps, detonationTimestamps,
                dataPduTimestamps, actionRequestTimestamps, startResumeTimestamps, setDataTimestamps,
                designatorTimestamps, electromagneticEmissionsTimestamps)
                .reduce(Stream::concat)
                .orElseGet(Stream::empty)
                .sorted()
                .collect(Collectors.toList());

        if (allPduEpochSeconds.isEmpty()) {
            return new AggregatedMetricsOverview.PeakLoadInfo(0.0, windowStartTimeUtc, windowEndTimeUtc, 0);
        }

        Map<Long, Long> packetsPerMinuteInterval = new HashMap<>();
        long intervalSeconds = 60; // 1-minute intervals

        for (Long pduEpochSecond : allPduEpochSeconds) {
            long minuteBucketStartEpochSecond = (pduEpochSecond / intervalSeconds) * intervalSeconds;
            packetsPerMinuteInterval.put(minuteBucketStartEpochSecond,
                    packetsPerMinuteInterval.getOrDefault(minuteBucketStartEpochSecond, 0L) + 1);
        }

        if (packetsPerMinuteInterval.isEmpty()) {
             return new AggregatedMetricsOverview.PeakLoadInfo(0.0, windowStartTimeUtc, windowEndTimeUtc, 0);
        }

        long maxPacketsInInterval = 0;
        long peakIntervalStartEpochSecond = windowStartTimeUtc.getEpochSecond(); // Default to window start

        for (Map.Entry<Long, Long> entry : packetsPerMinuteInterval.entrySet()) {
            if (entry.getValue() > maxPacketsInInterval) {
                maxPacketsInInterval = entry.getValue();
                peakIntervalStartEpochSecond = entry.getKey();
            }
        }
        
        double peakPacketsPerSecond = (maxPacketsInInterval > 0 && intervalSeconds > 0) ? ((double) maxPacketsInInterval / intervalSeconds) : 0.0;
        Instant peakIntervalStartUtc = Instant.ofEpochSecond(peakIntervalStartEpochSecond);
        Instant peakIntervalEndUtc = peakIntervalStartUtc.plusSeconds(intervalSeconds);
        
        // Ensure the reported interval is within the overall query window
        if(peakIntervalStartUtc.isBefore(windowStartTimeUtc)){
             peakIntervalStartUtc = windowStartTimeUtc;
        }
        if(peakIntervalEndUtc.isAfter(windowEndTimeUtc)){
            peakIntervalEndUtc = windowEndTimeUtc;
        }
        // Recalculate maxPacketsInInterval if the interval was clipped (more complex, omitted for now, assumes full minute peak is most relevant)

        return new AggregatedMetricsOverview.PeakLoadInfo(
                peakPacketsPerSecond,
                peakIntervalStartUtc,
                peakIntervalEndUtc,
                maxPacketsInInterval // This count is for the full bucket, not potentially clipped interval
        );
    }

    // --- New method for fetching all PDU logs within a time range ---
    public PduLogResponse getAllPduLogs(Long startTime, Long endTime) {
        log.info("Fetching all PDU logs between DIS TS: {} and {}", startTime, endTime);
        
        List<PduLogResponse.PduLogEntry> pduMessages = new ArrayList<>();
        
        // Fetch all PDU types within the time range
        List<EntityStateRecord> entityStates = entityStateRepository.findByTimestampBetween(startTime, endTime);
        List<FireEventRecord> fireEvents = fireEventRepository.findByTimestampBetween(startTime, endTime);
        List<CollisionRecord> collisionEvents = collisionRepository.findByTimestampBetween(startTime, endTime);
        List<DetonationRecord> detonationEvents = detonationRepository.findByTimestampBetween(startTime, endTime);
        List<DataPduRecord> dataPduEvents = dataPduRepository.findByTimestampBetween(startTime, endTime);
        List<ActionRequestPduRecord> actionRequestEvents = actionRequestPduRepository.findByTimestampBetween(startTime, endTime);
        List<StartResumePduRecord> startResumeEvents = startResumePduRepository.findByTimestampBetween(startTime, endTime);
        List<SetDataPduRecord> setDataEvents = setDataPduRepository.findByTimestampBetween(startTime, endTime);
        List<DesignatorPduRecord> designatorEvents = designatorPduRepository.findByTimestampBetween(startTime, endTime);
        List<ElectromagneticEmissionsPduRecord> electromagneticEmissionsEvents = electromagneticEmissionsPduRepository.findByTimestampBetween(startTime, endTime);
        
        // Convert EntityState records
        if (entityStates != null) {
            for (EntityStateRecord record : entityStates) {
                Map<String, Object> recordDetails = new HashMap<>();
                recordDetails.put("site", record.getSite());
                recordDetails.put("application", record.getApplication());
                recordDetails.put("entity", record.getEntity());
                recordDetails.put("locationX", record.getLocationX());
                recordDetails.put("locationY", record.getLocationY());
                recordDetails.put("locationZ", record.getLocationZ());
                recordDetails.put("timestamp", record.getTimestamp()); // Keep original DIS timestamp for compatibility
                recordDetails.put("timestampEpoch", fromDisAbsoluteTimestamp(record.getTimestamp())); // Unix epoch seconds
                recordDetails.put("timestampHuman", formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(record.getTimestamp())))); // Human readable
                
                pduMessages.add(new PduLogResponse.PduLogEntry(
                    record.getId(),
                    "EntityState",
                    calculatePduLength("EntityState", recordDetails),
                    recordDetails
                ));
            }
        }
        
        // Convert FireEvent records
        if (fireEvents != null) {
            for (FireEventRecord record : fireEvents) {
                Map<String, Object> recordDetails = new HashMap<>();
                recordDetails.put("firingSite", record.getFiringSite());
                recordDetails.put("firingApplication", record.getFiringApplication());
                recordDetails.put("firingEntity", record.getFiringEntity());
                recordDetails.put("targetSite", record.getTargetSite());
                recordDetails.put("targetApplication", record.getTargetApplication());
                recordDetails.put("targetEntity", record.getTargetEntity());
                recordDetails.put("munitionSite", record.getMunitionSite());
                recordDetails.put("munitionApplication", record.getMunitionApplication());
                recordDetails.put("munitionEntity", record.getMunitionEntity());
                recordDetails.put("timestamp", record.getTimestamp()); // Keep original DIS timestamp for compatibility
                recordDetails.put("timestampEpoch", fromDisAbsoluteTimestamp(record.getTimestamp())); // Unix epoch seconds
                recordDetails.put("timestampHuman", formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(record.getTimestamp())))); // Human readable
                
                pduMessages.add(new PduLogResponse.PduLogEntry(
                    record.getId(),
                    "FireEvent",
                    calculatePduLength("FireEvent", recordDetails),
                    recordDetails
                ));
            }
        }
        
        // Convert Collision records
        if (collisionEvents != null) {
            for (CollisionRecord record : collisionEvents) {
                Map<String, Object> recordDetails = new HashMap<>();
                recordDetails.put("issuingSite", record.getIssuingSite());
                recordDetails.put("issuingApplication", record.getIssuingApplication());
                recordDetails.put("issuingEntity", record.getIssuingEntity());
                recordDetails.put("collidingSite", record.getCollidingSite());
                recordDetails.put("collidingApplication", record.getCollidingApplication());
                recordDetails.put("collidingEntity", record.getCollidingEntity());
                recordDetails.put("timestamp", record.getTimestamp()); // Keep original DIS timestamp for compatibility
                recordDetails.put("timestampEpoch", fromDisAbsoluteTimestamp(record.getTimestamp())); // Unix epoch seconds
                recordDetails.put("timestampHuman", formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(record.getTimestamp())))); // Human readable
                
                pduMessages.add(new PduLogResponse.PduLogEntry(
                    record.getId(),
                    "Collision",
                    calculatePduLength("Collision", recordDetails),
                    recordDetails
                ));
            }
        }
        
        // Convert Detonation records
        if (detonationEvents != null) {
            for (DetonationRecord record : detonationEvents) {
                Map<String, Object> recordDetails = new HashMap<>();
                recordDetails.put("firingSite", record.getFiringSite());
                recordDetails.put("firingApplication", record.getFiringApplication());
                recordDetails.put("firingEntity", record.getFiringEntity());
                recordDetails.put("targetSite", record.getTargetSite());
                recordDetails.put("targetApplication", record.getTargetApplication());
                recordDetails.put("targetEntity", record.getTargetEntity());
                recordDetails.put("locationX", record.getLocationX());
                recordDetails.put("locationY", record.getLocationY());
                recordDetails.put("locationZ", record.getLocationZ());
                recordDetails.put("timestamp", record.getTimestamp());
                
                pduMessages.add(new PduLogResponse.PduLogEntry(
                    record.getId(),
                    "Detonation",
                    calculatePduLength("Detonation", recordDetails),
                    recordDetails
                ));
            }
        }
        
        // Convert DataPdu records
        if (dataPduEvents != null) {
            for (DataPduRecord record : dataPduEvents) {
                Map<String, Object> recordDetails = new HashMap<>();
                recordDetails.put("originatingSite", record.getOriginatingSite());
                recordDetails.put("originatingApplication", record.getOriginatingApplication());
                recordDetails.put("originatingEntity", record.getOriginatingEntity());
                recordDetails.put("receivingSite", record.getReceivingSite());
                recordDetails.put("receivingApplication", record.getReceivingApplication());
                recordDetails.put("receivingEntity", record.getReceivingEntity());
                recordDetails.put("timestamp", record.getTimestamp()); // Keep original DIS timestamp for compatibility
                recordDetails.put("timestampEpoch", fromDisAbsoluteTimestamp(record.getTimestamp())); // Unix epoch seconds
                recordDetails.put("timestampHuman", formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(record.getTimestamp())))); // Human readable
                
                pduMessages.add(new PduLogResponse.PduLogEntry(
                    record.getId(),
                    "DataPdu",
                    calculatePduLength("DataPdu", recordDetails),
                    recordDetails
                ));
            }
        }
        
        // Convert ActionRequest records
        if (actionRequestEvents != null) {
            for (ActionRequestPduRecord record : actionRequestEvents) {
                Map<String, Object> recordDetails = new HashMap<>();
                recordDetails.put("originatingSite", record.getOriginatingSite());
                recordDetails.put("originatingApplication", record.getOriginatingApplication());
                recordDetails.put("originatingEntity", record.getOriginatingEntity());
                recordDetails.put("receivingSite", record.getReceivingSite());
                recordDetails.put("receivingApplication", record.getReceivingApplication());
                recordDetails.put("receivingEntity", record.getReceivingEntity());
                recordDetails.put("timestamp", record.getTimestamp());
                
                pduMessages.add(new PduLogResponse.PduLogEntry(
                    record.getId(),
                    "ActionRequest",
                    calculatePduLength("ActionRequest", recordDetails),
                    recordDetails
                ));
            }
        }
        
        // Convert StartResume records
        if (startResumeEvents != null) {
            for (StartResumePduRecord record : startResumeEvents) {
                Map<String, Object> recordDetails = new HashMap<>();
                recordDetails.put("hour", record.getHour());
                recordDetails.put("timePastHour", record.getTimePastHour());
                recordDetails.put("timestamp", record.getTimestamp()); // Keep original DIS timestamp for compatibility
                recordDetails.put("timestampEpoch", fromDisAbsoluteTimestamp(record.getTimestamp())); // Unix epoch seconds
                recordDetails.put("timestampHuman", formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(record.getTimestamp())))); // Human readable
                
                pduMessages.add(new PduLogResponse.PduLogEntry(
                    record.getId(),
                    "StartResume",
                    calculatePduLength("StartResume", recordDetails),
                    recordDetails
                ));
            }
        }
        
        // Convert SetData records
        if (setDataEvents != null) {
            for (SetDataPduRecord record : setDataEvents) {
                Map<String, Object> recordDetails = new HashMap<>();
                recordDetails.put("originatingSite", record.getOriginatingSite());
                recordDetails.put("originatingApplication", record.getOriginatingApplication());
                recordDetails.put("originatingEntity", record.getOriginatingEntity());
                recordDetails.put("timestamp", record.getTimestamp()); // Keep original DIS timestamp for compatibility
                recordDetails.put("timestampEpoch", fromDisAbsoluteTimestamp(record.getTimestamp())); // Unix epoch seconds
                recordDetails.put("timestampHuman", formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(record.getTimestamp())))); // Human readable
                
                pduMessages.add(new PduLogResponse.PduLogEntry(
                    record.getId(),
                    "SetData",
                    calculatePduLength("SetData", recordDetails),
                    recordDetails
                ));
            }
        }
        
        // Convert Designator records
        if (designatorEvents != null) {
            for (DesignatorPduRecord record : designatorEvents) {
                Map<String, Object> recordDetails = new HashMap<>();
                recordDetails.put("designatingSite", record.getDesignatingSite());
                recordDetails.put("designatingApplication", record.getDesignatingApplication());
                recordDetails.put("designatingEntity", record.getDesignatingEntity());
                recordDetails.put("timestamp", record.getTimestamp()); // Keep original DIS timestamp for compatibility
                recordDetails.put("timestampEpoch", fromDisAbsoluteTimestamp(record.getTimestamp())); // Unix epoch seconds
                recordDetails.put("timestampHuman", formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(record.getTimestamp())))); // Human readable
                
                pduMessages.add(new PduLogResponse.PduLogEntry(
                    record.getId(),
                    "Designator",
                    calculatePduLength("Designator", recordDetails),
                    recordDetails
                ));
            }
        }
        
        // Convert ElectromagneticEmissions records
        if (electromagneticEmissionsEvents != null) {
            for (ElectromagneticEmissionsPduRecord record : electromagneticEmissionsEvents) {
                Map<String, Object> recordDetails = new HashMap<>();
                recordDetails.put("emittingSite", record.getEmittingSite());
                recordDetails.put("emittingApplication", record.getEmittingApplication());
                recordDetails.put("emittingEntity", record.getEmittingEntity());
                recordDetails.put("timestamp", record.getTimestamp()); // Keep original DIS timestamp for compatibility
                recordDetails.put("timestampEpoch", fromDisAbsoluteTimestamp(record.getTimestamp())); // Unix epoch seconds
                recordDetails.put("timestampHuman", formatInstant(Instant.ofEpochSecond(fromDisAbsoluteTimestamp(record.getTimestamp())))); // Human readable
                
                pduMessages.add(new PduLogResponse.PduLogEntry(
                    record.getId(),
                    "ElectromagneticEmissions",
                    calculatePduLength("ElectromagneticEmissions", recordDetails),
                    recordDetails
                ));
            }
        }
        
        log.info("Returning {} PDU log entries", pduMessages.size());
        return new PduLogResponse(pduMessages);
    }
    
    // Helper method to calculate PDU length based on type and content
    private int calculatePduLength(String pduType, Map<String, Object> recordDetails) {
        // Base PDU header length (common to all PDUs)
        int baseLength = 12; // PDU header
        
        switch (pduType) {
            case "EntityState":
                return baseLength + 144; // EntityState PDU typical length
            case "FireEvent":
                return baseLength + 96; // Fire PDU typical length
            case "Collision":
                return baseLength + 56; // Collision PDU typical length
            case "Detonation":
                return baseLength + 104; // Detonation PDU typical length
            case "DataPdu":
                return baseLength + 64; // Data PDU typical length
            case "ActionRequest":
                return baseLength + 64; // Action Request PDU typical length
            case "StartResume":
                return baseLength + 40; // Start/Resume PDU typical length
            case "SetData":
                return baseLength + 32; // Set Data PDU typical length
            case "Designator":
                return baseLength + 88; // Designator PDU typical length
            case "ElectromagneticEmissions":
                return baseLength + 72; // Electromagnetic Emissions PDU typical length
            default:
                return baseLength + 32; // Default length for unknown PDU types
        }
    }
}