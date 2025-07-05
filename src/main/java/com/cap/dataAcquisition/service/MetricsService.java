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
}