package com.cap.dataAcquisition.service;

import com.cap.dataAcquisition.model.AggregatedMetricsOverview;
import com.cap.dataAcquisition.model.EntityStateRecord;
import com.cap.dataAcquisition.model.FireEventRecord;
import com.cap.dataAcquisition.repository.EntityStateRepository;
import com.cap.dataAcquisition.repository.FireEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MetricsServiceTest {

    @Mock
    private EntityStateRepository entityStateRepository;

    @Mock
    private FireEventRepository fireEventRepository;

    @InjectMocks
    private MetricsService metricsService;

    private List<EntityStateRecord> entityStates;
    private List<FireEventRecord> fireEvents;

    @BeforeEach
    void setUp() {
        entityStates = new ArrayList<>();
        fireEvents = new ArrayList<>();
    }

    // --- Test Static Helper Methods ---

    @Test
    void testToDisAbsoluteTimestamp() {
        long epochSeconds = 1678886400L; // Example epoch second
        long expectedDisTimestamp = epochSeconds | 0x80000000L; // [cite: 35]
        assertEquals(expectedDisTimestamp, MetricsService.toDisAbsoluteTimestamp(epochSeconds));
    }

    @Test
    void testFromDisAbsoluteTimestamp_withMsbSet() {
        long epochSeconds = 1678886400L;
        long disTimestamp = epochSeconds | 0x80000000L; // [cite: 35]
        assertEquals(epochSeconds, MetricsService.fromDisAbsoluteTimestamp(disTimestamp)); // [cite: 35]
    }

    @Test
    void testFromDisAbsoluteTimestamp_withoutMsbSet() {
        long notReallyDisTimestamp = 1678886400L; // MSB not set
        // Should log a warning (source: 36) and return as is (source: 37)
        assertEquals(notReallyDisTimestamp, MetricsService.fromDisAbsoluteTimestamp(notReallyDisTimestamp));
    }

    @Test
    void testFormatInstant_validInstant() {
        Instant now = Instant.now();
        String formatted = MetricsService.formatInstant(now); // [cite: 37, 38]
        assertNotNull(formatted);
        assertTrue(formatted.endsWith("Z"), "Formatted string should end with Z for UTC"); // [cite: 38]
    }

    @Test
    void testFormatInstant_nullInstant() {
        assertEquals("N/A", MetricsService.formatInstant(null)); // [cite: 37]
    }

    // --- Helper methods for creating PDU records for tests ---
    private EntityStateRecord createEntityState(long disTimestamp) {
        EntityStateRecord esr = new EntityStateRecord();
        esr.setTimestamp(disTimestamp);
        // Set other fields if they become relevant for peak load or other calculations
        return esr;
    }

    private FireEventRecord createFireEvent(long disTimestamp) {
        FireEventRecord fer = new FireEventRecord();
        fer.setTimestamp(disTimestamp);
        // Set other fields if relevant
        return fer;
    }

    // --- Tests for getAggregatedMetrics and calculatePeakLoad ---

    @Test
    void getAggregatedMetrics_last60minutes_noData() {
        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList()); // [cite: 47]
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList()); // [cite: 47]

        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("last60minutes"); // [cite: 39]

        assertEquals("Last 60 minutes", overview.getTimeWindowDescription()); // [cite: 40]
        assertEquals(0, overview.getTotalPackets()); // [cite: 48]
        assertEquals(0.0, overview.getAveragePacketsPerSecond()); // [cite: 49]
        assertNotNull(overview.getPeakLoad());
        assertEquals(0.0, overview.getPeakLoad().getPeakPacketsPerSecond()); // [cite: 56]
        assertEquals(0, overview.getPeakLoad().getPacketsInPeakInterval()); // [cite: 56]
    }

    @Test
    void getAggregatedMetrics_lastDay_withData_correctPeakCalculation() {
        Instant now = Instant.now();
        long nowEpochSeconds = now.getEpochSecond();

        // Define a base for our peak minute, ensuring it's a clear minute boundary
        long peakMinuteStartEpoch = ((nowEpochSeconds - (50 * 60)) / 60) * 60; // Roughly 50 minutes ago, aligned to a minute start

        // These three timestamps will fall into the same 60-second bucket
        long epoch_ts1_in_peak_minute = peakMinuteStartEpoch + 5;  // 5 seconds into the peak minute
        long epoch_ts2_in_peak_minute = peakMinuteStartEpoch + 15; // 15 seconds into the peak minute
        long epoch_ts3_in_peak_minute = peakMinuteStartEpoch + 25; // 25 seconds into the peak minute

        // This timestamp will be outside the peak minute, e.g., 2 hours before the peak minute
        long epoch_ts4_outside_peak_minute = peakMinuteStartEpoch - (2 * 60 * 60);

        // Convert to DIS Timestamps (MSB set)
        long dis_ts1 = MetricsService.toDisAbsoluteTimestamp(epoch_ts1_in_peak_minute);
        long dis_ts2 = MetricsService.toDisAbsoluteTimestamp(epoch_ts2_in_peak_minute);
        long dis_ts3 = MetricsService.toDisAbsoluteTimestamp(epoch_ts3_in_peak_minute);
        long dis_ts4 = MetricsService.toDisAbsoluteTimestamp(epoch_ts4_outside_peak_minute);

        entityStates.add(createEntityState(dis_ts1));
        entityStates.add(createEntityState(dis_ts4)); // ts4 is outside the peak minute but within "lastDay"
        fireEvents.add(createFireEvent(dis_ts2));
        fireEvents.add(createFireEvent(dis_ts3));

        // Mock repository calls
        // The findByTimestampBetween will be called with start/end times for "lastDay"
        // We ensure our test data (dis_ts1 to dis_ts4) will fall within this window.
        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(entityStates); // [cite: 47]
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(fireEvents); // [cite: 47]

        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("lastDay"); // [cite: 40]

        assertEquals("Last 24 hours", overview.getTimeWindowDescription()); // [cite: 41]
        assertEquals(4, overview.getTotalPackets()); // [cite: 48]
        long durationSecondsForLastDay = 24 * 60 * 60;
        assertEquals(4.0 / durationSecondsForLastDay, overview.getAveragePacketsPerSecond(), 0.0001); // [cite: 49]

        assertNotNull(overview.getPeakLoad()); // [cite: 50]
        assertEquals(3, overview.getPeakLoad().getPacketsInPeakInterval(), "Expected 3 packets in the peak interval"); // [cite: 68]
        assertEquals(3.0 / 60.0, overview.getPeakLoad().getPeakPacketsPerSecond(), 0.0001); // [cite: 64]

        // Verify peak interval times
        Instant expectedPeakStart = Instant.ofEpochSecond(peakMinuteStartEpoch);
        assertEquals(expectedPeakStart, overview.getPeakLoad().getPeakIntervalStartUtc()); // [cite: 63, 64]
        assertEquals(expectedPeakStart.plusSeconds(60), overview.getPeakLoad().getPeakIntervalEndUtc()); // [cite: 64]
    }

    @Test
    void getAggregatedMetrics_unsupportedPeriod_defaultsToLast60Minutes() {
        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("unsupportedPeriod"); // [cite: 43]
        assertEquals("Last 60 minutes (default)", overview.getTimeWindowDescription()); // [cite: 42]
    }

    @Test
    void calculatePeakLoad_clipsIntervalToWindowIfNecessary() {
        Instant now = Instant.now();
        // Window for "last60minutes"
        Instant windowStartTime = now.minus(60, ChronoUnit.MINUTES); // [cite: 39]
        Instant windowEndTime = now;

        // Create a packet timestamp that would place its 60s bucket start *before* the windowStartTime
        long packetEpochSecond = windowStartTime.minusSeconds(30).getEpochSecond(); // 30s before window starts
        long disPacketTimestamp = MetricsService.toDisAbsoluteTimestamp(packetEpochSecond);

        entityStates.add(createEntityState(disPacketTimestamp));

        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(entityStates);
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        // Requesting for "last10minutes" will default to "last60minutes" in the current MetricsService
        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("last10minutes"); // Will use default [cite: 42, 43]

        AggregatedMetricsOverview.PeakLoadInfo peakLoadInfo = overview.getPeakLoad(); // [cite: 50]

        // The raw bucket for packetEpochSecond would start at (packetEpochSecond / 60) * 60.
        // However, the reported peakIntervalStartUtc should be clipped at windowStartTime if the bucket is outside.
        // And peakIntervalEndUtc should be clipped at windowEndTime.
        // Source [cite: 65, 66] handles this clipping.
        assertNotNull(peakLoadInfo);
        assertFalse(peakLoadInfo.getPeakIntervalStartUtc().isBefore(overview.getDataFromUtc()),
                "Peak interval start should not be before the data window start.");
        assertFalse(peakLoadInfo.getPeakIntervalEndUtc().isAfter(overview.getDataUntilUtc()),
                "Peak interval end should not be after the data window end.");

        // If the peak bucket was entirely outside and before, after clipping,
        // the effective interval for counting packets within the window might be shorter or yield 0.
        // The current logic (source: 68) returns maxPacketsInInterval for the *full bucket*,
        // not the clipped interval, which is a point noted in the source as "more complex, omitted for now".
        // So, if the bucket containing the point is partially outside, the count might still be 1 here.
        // If the true peak bucket is entirely outside the window, the behavior of what is reported as peak might need further clarification.
        // Given the current clipping, if a packet leads to a bucket start before window, and that bucket is selected as peak:
        // PeakLoadInfo will have peakIntervalStartUtc = windowStartTime.
        // And packetsInPeakInterval will be based on the original bucket count.
        if (peakLoadInfo.getPacketsInPeakInterval() > 0) {
             assertTrue(peakLoadInfo.getPeakPacketsPerSecond() > 0);
        }
    }

    @Test
    void calculatePeakLoad_emptyPduList() {
         when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("last60minutes");
        AggregatedMetricsOverview.PeakLoadInfo peakLoadInfo = overview.getPeakLoad();

        assertEquals(0.0, peakLoadInfo.getPeakPacketsPerSecond()); // [cite: 56]
        assertEquals(0, peakLoadInfo.getPacketsInPeakInterval()); // [cite: 56]
        assertEquals(overview.getDataFromUtc(), peakLoadInfo.getPeakIntervalStartUtc()); // [cite: 56]
        assertEquals(overview.getDataUntilUtc(), peakLoadInfo.getPeakIntervalEndUtc()); // [cite: 56]
    }

    @Test
    void calculatePeakLoad_singlePdu() {
        Instant now = Instant.now();
        long packetTime = now.minus(30, ChronoUnit.MINUTES).getEpochSecond();
        entityStates.add(createEntityState(MetricsService.toDisAbsoluteTimestamp(packetTime)));

        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(entityStates);
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("last60minutes");
        AggregatedMetricsOverview.PeakLoadInfo peakLoadInfo = overview.getPeakLoad();

        assertEquals(1, peakLoadInfo.getPacketsInPeakInterval()); // [cite: 59]
        assertEquals(1.0/60.0, peakLoadInfo.getPeakPacketsPerSecond(), 0.00001); // [cite: 64]
        long expectedBucketStart = (packetTime / 60) * 60;
        assertEquals(Instant.ofEpochSecond(expectedBucketStart), peakLoadInfo.getPeakIntervalStartUtc()); // [cite: 63]
    }
}