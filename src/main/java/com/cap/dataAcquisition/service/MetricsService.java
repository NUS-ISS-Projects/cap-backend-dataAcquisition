package com.cap.dataAcquisition.service;

import com.cap.dataAcquisition.model.AggregatedMetricsOverview;
import com.cap.dataAcquisition.model.EntityStateRecord;
import com.cap.dataAcquisition.model.FireEventRecord;
import com.cap.dataAcquisition.repository.EntityStateRepository;
import com.cap.dataAcquisition.repository.FireEventRepository;
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

    @Autowired
    public MetricsService(EntityStateRepository entityStateRepository, FireEventRepository fireEventRepository) {
        this.entityStateRepository = entityStateRepository;
        this.fireEventRepository = fireEventRepository;
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

        long totalEntityStatePackets = entityStates != null ? entityStates.size() : 0;
        long totalFireEventPackets = fireEvents != null ? fireEvents.size() : 0;
        long totalPackets = totalEntityStatePackets + totalFireEventPackets;

        double durationSeconds = ChronoUnit.SECONDS.between(startTimeUtc, endTimeUtc);
        double averagePacketsPerSecond = (durationSeconds > 0) ? (totalPackets / durationSeconds) : 0.0;

        AggregatedMetricsOverview.PeakLoadInfo peakLoadInfo = calculatePeakLoad(
                entityStates, fireEvents, startTimeUtc, endTimeUtc
        );

        return new AggregatedMetricsOverview(
                timeWindowDescription,
                startTimeUtc,
                endTimeUtc,
                totalPackets,
                averagePacketsPerSecond,
                peakLoadInfo
        );
    }

    private AggregatedMetricsOverview.PeakLoadInfo calculatePeakLoad(
            List<EntityStateRecord> entityStates,
            List<FireEventRecord> fireEvents,
            Instant windowStartTimeUtc,
            Instant windowEndTimeUtc) {

        // Explicit typing in map can help with "capture#1-of ? extends Object" issues if Lombok/IDE struggles
        Stream<Long> entityTimestamps = (entityStates != null ? entityStates.stream() : Stream.<EntityStateRecord>empty())
                .map((EntityStateRecord record) -> fromDisAbsoluteTimestamp(record.getTimestamp()));
        Stream<Long> fireTimestamps = (fireEvents != null ? fireEvents.stream() : Stream.<FireEventRecord>empty())
                .map((FireEventRecord record) -> fromDisAbsoluteTimestamp(record.getTimestamp()));

        List<Long> allPduEpochSeconds = Stream.concat(entityTimestamps, fireTimestamps)
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