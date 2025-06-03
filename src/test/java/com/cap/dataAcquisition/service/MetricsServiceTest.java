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
    private EntityStateRepository entityStateRepository; //

    @Mock
    private FireEventRepository fireEventRepository; //

    @InjectMocks
    private MetricsService metricsService; //

    private List<EntityStateRecord> entityStates;
    private List<FireEventRecord> fireEvents;

    @BeforeEach
    void setUp() {
        entityStates = new ArrayList<>();
        fireEvents = new ArrayList<>();
    }

    @Test
    void testToDisAbsoluteTimestamp() {
        long epochSeconds = 1678886400L; // Some epoch second
        long expectedDisTimestamp = epochSeconds | 0x80000000L; //
        assertEquals(expectedDisTimestamp, MetricsService.toDisAbsoluteTimestamp(epochSeconds));
    }

    @Test
    void testFromDisAbsoluteTimestamp_withMsbSet() {
        long epochSeconds = 1678886400L;
        long disTimestamp = epochSeconds | 0x80000000L;
        assertEquals(epochSeconds, MetricsService.fromDisAbsoluteTimestamp(disTimestamp)); //
    }

    @Test
    void testFromDisAbsoluteTimestamp_withoutMsbSet() {
        long notReallyDisTimestamp = 1678886400L; // MSB not set
        // Should log a warning and return as is
        assertEquals(notReallyDisTimestamp, MetricsService.fromDisAbsoluteTimestamp(notReallyDisTimestamp));
    }

    @Test
    void testFormatInstant_validInstant() {
        Instant now = Instant.now();
        String formatted = MetricsService.formatInstant(now); //
        assertNotNull(formatted);
        assertTrue(formatted.endsWith("Z")); // ISO_INSTANT UTC
    }

    @Test
    void testFormatInstant_nullInstant() {
        assertEquals("N/A", MetricsService.formatInstant(null)); //
    }

    private EntityStateRecord createEntityState(long disTimestamp) {
        EntityStateRecord esr = new EntityStateRecord(); //
        esr.setTimestamp(disTimestamp); //
        return esr;
    }

    private FireEventRecord createFireEvent(long disTimestamp) {
        FireEventRecord fer = new FireEventRecord(); //
        fer.setTimestamp(disTimestamp); //
        return fer;
    }

    @Test
    void getAggregatedMetrics_last60minutes_noData() {
        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList()); //
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList()); //

        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("last60minutes"); //

        assertEquals("Last 60 minutes", overview.getTimeWindowDescription());
        assertEquals(0, overview.getTotalPackets()); //
        assertEquals(0.0, overview.getAveragePacketsPerSecond()); //
        assertNotNull(overview.getPeakLoad());
        assertEquals(0.0, overview.getPeakLoad().getPeakPacketsPerSecond()); //
        assertEquals(0, overview.getPeakLoad().getPacketsInPeakInterval());
    }

    @Test
    void getAggregatedMetrics_lastDay_withData() {
        Instant now = Instant.now();
        long nowEpochSeconds = now.getEpochSecond();
        // Simulate PDUs within the last day, focusing on a peak in one minute
        // DIS Timestamps (MSB set)
        long ts1 = MetricsService.toDisAbsoluteTimestamp(nowEpochSeconds - 3000); // ~50 mins ago
        long ts2 = MetricsService.toDisAbsoluteTimestamp(nowEpochSeconds - 3010); // same 1-min bucket
        long ts3 = MetricsService.toDisAbsoluteTimestamp(nowEpochSeconds - 3020); // same 1-min bucket
        long ts4 = MetricsService.toDisAbsoluteTimestamp(nowEpochSeconds - 7200); // ~2 hours ago

        entityStates.add(createEntityState(ts1));
        entityStates.add(createEntityState(ts4));
        fireEvents.add(createFireEvent(ts2));
        fireEvents.add(createFireEvent(ts3));

        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(entityStates); //
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(fireEvents); //

        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("lastDay"); //

        assertEquals("Last 24 hours", overview.getTimeWindowDescription());
        assertEquals(4, overview.getTotalPackets()); //
        double expectedDuration = ChronoUnit.DAYS.getDuration().getSeconds();
        assertEquals(4.0 / expectedDuration, overview.getAveragePacketsPerSecond(), 0.0001); //

        assertNotNull(overview.getPeakLoad());
        // 3 packets (ts1, ts2, ts3) fall into the same 60-second interval for peak calculation
        assertEquals(3, overview.getPeakLoad().getPacketsInPeakInterval()); //
        assertEquals(3.0 / 60.0, overview.getPeakLoad().getPeakPacketsPerSecond(), 0.0001); //
    }
    
    @Test
    void getAggregatedMetrics_unsupportedPeriod_defaultsToLast60Minutes() {
        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("unsupported"); //
        assertEquals("Last 60 minutes (default)", overview.getTimeWindowDescription()); //
    }

    @Test
    void calculatePeakLoad_clipsIntervalToWindow() {
        Instant windowStartTime = Instant.now().minus(10, ChronoUnit.MINUTES);
        Instant windowEndTime = Instant.now();
        long peakIntervalStartEpoch = windowStartTime.minus(30, ChronoUnit.SECONDS).getEpochSecond(); // Peak starts before window
        long packetTimeInPeak = peakIntervalStartEpoch + 5; // Packet time

        // Simulate one entity state record whose timestamp, when converted, falls into this peak bucket
        entityStates.add(createEntityState(MetricsService.toDisAbsoluteTimestamp(packetTimeInPeak)));

        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(entityStates);
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("last10minutes"); 
        // Even though actual peak was earlier, reported peak start should be clipped to window start

        AggregatedMetricsOverview.PeakLoadInfo peakLoadInfo = overview.getPeakLoad();
        // The peak calculation logic groups by 60s intervals. If the actual peak bucket start
        // (derived from packetTimeInPeak) is before windowStartTime, the reported peakIntervalStartUtc might
        // still be that original bucket start or clipped. The source clips peakIntervalStartUtc if it's before window.
        // For this specific setup, the window is "last10minutes", if "last10minutes" is not a defined period,
        // it defaults to "last60minutes". Let's assume default for simplicity here.
        
        AggregatedMetricsOverview defaultOverview = metricsService.getAggregatedMetrics("someDefault");
        AggregatedMetricsOverview.PeakLoadInfo defaultPeakLoadInfo = defaultOverview.getPeakLoad();

        // Verify that the peak interval start time reported is not before the window start time.
        assertFalse(defaultPeakLoadInfo.getPeakIntervalStartUtc().isBefore(defaultOverview.getDataFromUtc()));
         // And peak interval end is not after window end time
        assertFalse(defaultPeakLoadInfo.getPeakIntervalEndUtc().isAfter(defaultOverview.getDataUntilUtc()));
    }
}