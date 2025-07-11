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
    
    @Mock
    private CollisionRepository collisionRepository;
    
    @Mock
    private DetonationRepository detonationRepository;
    
    @Mock
    private DataPduRepository dataPduRepository;
    
    @Mock
    private ActionRequestPduRepository actionRequestPduRepository;
    
    @Mock
    private StartResumePduRepository startResumePduRepository;
    
    @Mock
    private SetDataPduRepository setDataPduRepository;
    
    @Mock
    private DesignatorPduRepository designatorPduRepository;
    
    @Mock
    private ElectromagneticEmissionsPduRepository electromagneticEmissionsPduRepository;

    @InjectMocks
    private MetricsService metricsService;

    private List<EntityStateRecord> entityStates;
    private List<FireEventRecord> fireEvents;
    private List<CollisionRecord> collisionEvents;
    private List<DetonationRecord> detonationEvents;
    private List<DataPduRecord> dataPduEvents;
    private List<ActionRequestPduRecord> actionRequestEvents;
    private List<StartResumePduRecord> startResumeEvents;
    private List<SetDataPduRecord> setDataEvents;
    private List<DesignatorPduRecord> designatorEvents;
    private List<ElectromagneticEmissionsPduRecord> electromagneticEmissionsEvents;

    @BeforeEach
    void setUp() {
        entityStates = new ArrayList<>();
        fireEvents = new ArrayList<>();
        collisionEvents = new ArrayList<>();
        detonationEvents = new ArrayList<>();
        dataPduEvents = new ArrayList<>();
        actionRequestEvents = new ArrayList<>();
        startResumeEvents = new ArrayList<>();
        setDataEvents = new ArrayList<>();
        designatorEvents = new ArrayList<>();
        electromagneticEmissionsEvents = new ArrayList<>();
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
    
    private CollisionRecord createCollision(long disTimestamp) {
        CollisionRecord cr = new CollisionRecord();
        cr.setTimestamp(disTimestamp);
        // Set other fields if relevant
        return cr;
    }
    
    private DetonationRecord createDetonation(long disTimestamp) {
        DetonationRecord dr = new DetonationRecord();
        dr.setTimestamp(disTimestamp);
        // Set other fields if relevant
        return dr;
    }
    
    private DataPduRecord createDataPdu(long disTimestamp) {
        DataPduRecord record = new DataPduRecord();
        record.setTimestamp(disTimestamp);
        return record;
    }
    
    private ActionRequestPduRecord createActionRequest(long disTimestamp) {
        ActionRequestPduRecord record = new ActionRequestPduRecord();
        record.setTimestamp(disTimestamp);
        return record;
    }
    
    private StartResumePduRecord createStartResume(long disTimestamp) {
        StartResumePduRecord record = new StartResumePduRecord();
        record.setTimestamp(disTimestamp);
        return record;
    }
    
    private SetDataPduRecord createSetData(long disTimestamp) {
        SetDataPduRecord record = new SetDataPduRecord();
        record.setTimestamp(disTimestamp);
        return record;
    }
    
    private DesignatorPduRecord createDesignator(long disTimestamp) {
        DesignatorPduRecord record = new DesignatorPduRecord();
        record.setTimestamp(disTimestamp);
        return record;
    }
    
    private ElectromagneticEmissionsPduRecord createElectromagneticEmissions(long disTimestamp) {
        ElectromagneticEmissionsPduRecord record = new ElectromagneticEmissionsPduRecord();
        record.setTimestamp(disTimestamp);
        return record;
    }

    // --- Tests for getAggregatedMetrics and calculatePeakLoad ---

    @Test
    void getAggregatedMetrics_last60minutes_noData() {
        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList()); // [cite: 47]
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList()); // [cite: 47]
        when(collisionRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(detonationRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(dataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(actionRequestPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(startResumePduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(setDataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(designatorPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(electromagneticEmissionsPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());


        

        
        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("last60minutes"); // [cite: 39]

        assertEquals("Last 60 minutes", overview.getTimeWindowDescription()); // [cite: 40]
        assertEquals(0, overview.getTotalPackets()); // [cite: 48]
        assertEquals(0.0, overview.getAveragePacketsPerSecond()); // [cite: 49]
        assertEquals(0, overview.getEntityStatePackets());
        assertEquals(0, overview.getFireEventPackets());
        assertEquals(0, overview.getCollisionPackets());
        assertEquals(0, overview.getDetonationPackets());
        assertEquals(0, overview.getDataPduPackets());
        assertEquals(0, overview.getActionRequestPduPackets());
        assertEquals(0, overview.getStartResumePduPackets());
        assertEquals(0, overview.getSetDataPduPackets());
        assertEquals(0, overview.getDesignatorPduPackets());
        assertEquals(0, overview.getElectromagneticEmissionsPduPackets());
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
        when(collisionRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(detonationRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(dataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(actionRequestPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(startResumePduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(setDataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(designatorPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(electromagneticEmissionsPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("lastDay"); // [cite: 40]

        assertEquals("Last 24 hours", overview.getTimeWindowDescription()); // [cite: 41]
        assertEquals(4, overview.getTotalPackets()); // [cite: 48]
        assertEquals(2, overview.getEntityStatePackets());
        assertEquals(2, overview.getFireEventPackets());
        assertEquals(0, overview.getCollisionPackets());
        assertEquals(0, overview.getDetonationPackets());
        assertEquals(0, overview.getDataPduPackets());
        assertEquals(0, overview.getActionRequestPduPackets());
        assertEquals(0, overview.getStartResumePduPackets());
        assertEquals(0, overview.getSetDataPduPackets());
        assertEquals(0, overview.getDesignatorPduPackets());
        assertEquals(0, overview.getElectromagneticEmissionsPduPackets());
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
        when(collisionRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(detonationRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(dataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(actionRequestPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(startResumePduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(setDataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(designatorPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(electromagneticEmissionsPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());

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
        when(collisionRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(detonationRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(dataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(actionRequestPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(startResumePduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(setDataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(designatorPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(electromagneticEmissionsPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());

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
        when(collisionRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(detonationRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(dataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(actionRequestPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(startResumePduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(setDataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(designatorPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(electromagneticEmissionsPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());

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
        when(collisionRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(detonationRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(dataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(actionRequestPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(startResumePduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(setDataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(designatorPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());
        when(electromagneticEmissionsPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("last60minutes");
        AggregatedMetricsOverview.PeakLoadInfo peakLoadInfo = overview.getPeakLoad();

        assertEquals(1, peakLoadInfo.getPacketsInPeakInterval()); // [cite: 59]
        assertEquals(1.0/60.0, peakLoadInfo.getPeakPacketsPerSecond(), 0.00001); // [cite: 64]
        long expectedBucketStart = (packetTime / 60) * 60;
        assertEquals(Instant.ofEpochSecond(expectedBucketStart), peakLoadInfo.getPeakIntervalStartUtc()); // [cite: 63]
    }
    
    @Test
    void getAggregatedMetrics_withAllPduTypes() {
        Instant now = Instant.now();
        long nowEpochSeconds = now.getEpochSecond();
        
        // Create timestamps for different PDU types
        long entityStateTime = nowEpochSeconds - 300; // 5 minutes ago
        long fireEventTime = nowEpochSeconds - 240;   // 4 minutes ago
        long collisionTime = nowEpochSeconds - 180;   // 3 minutes ago
        long detonationTime = nowEpochSeconds - 120;  // 2 minutes ago
        long dataPduTime = nowEpochSeconds - 100;     // 1 minute 40 seconds ago
        
        // Make sure all 5 PDUs are in the same minute bucket by using the same timestamp
        // This ensures they'll be counted together in the peak load calculation
        long sameMinuteBucket = (nowEpochSeconds / 60) * 60; // Round down to the start of the current minute
        long actionRequestTime = sameMinuteBucket + 10; // 10 seconds into the minute
        long startResumeTime = sameMinuteBucket + 20;   // 20 seconds into the minute
        long setDataTime = sameMinuteBucket + 30;       // 30 seconds into the minute
        long designatorTime = sameMinuteBucket + 40;    // 40 seconds into the minute
        long emissionsTime = sameMinuteBucket + 50;     // 50 seconds into the minute
        
        // Convert to DIS Timestamps
        long disEntityStateTime = MetricsService.toDisAbsoluteTimestamp(entityStateTime);
        long disFireEventTime = MetricsService.toDisAbsoluteTimestamp(fireEventTime);
        long disCollisionTime = MetricsService.toDisAbsoluteTimestamp(collisionTime);
        long disDetonationTime = MetricsService.toDisAbsoluteTimestamp(detonationTime);
        long disDataPduTime = MetricsService.toDisAbsoluteTimestamp(dataPduTime);
        long disActionRequestTime = MetricsService.toDisAbsoluteTimestamp(actionRequestTime);
        long disStartResumeTime = MetricsService.toDisAbsoluteTimestamp(startResumeTime);
        long disSetDataTime = MetricsService.toDisAbsoluteTimestamp(setDataTime);
        long disDesignatorTime = MetricsService.toDisAbsoluteTimestamp(designatorTime);
        long disEmissionsTime = MetricsService.toDisAbsoluteTimestamp(emissionsTime);
        
        // Add records to their respective lists
        entityStates.add(createEntityState(disEntityStateTime));
        fireEvents.add(createFireEvent(disFireEventTime));
        collisionEvents.add(createCollision(disCollisionTime));
        detonationEvents.add(createDetonation(disDetonationTime));
        
        // Create helper methods for the new PDU types
        DataPduRecord dataPdu = new DataPduRecord();
        dataPdu.setTimestamp(disDataPduTime);
        dataPduEvents.add(dataPdu);
        
        ActionRequestPduRecord actionRequest = new ActionRequestPduRecord();
        actionRequest.setTimestamp(disActionRequestTime);
        actionRequestEvents.add(actionRequest);
        
        StartResumePduRecord startResume = new StartResumePduRecord();
        startResume.setTimestamp(disStartResumeTime);
        startResumeEvents.add(startResume);
        
        SetDataPduRecord setData = new SetDataPduRecord();
        setData.setTimestamp(disSetDataTime);
        setDataEvents.add(setData);
        
        DesignatorPduRecord designator = new DesignatorPduRecord();
        designator.setTimestamp(disDesignatorTime);
        designatorEvents.add(designator);
        
        ElectromagneticEmissionsPduRecord emissions = new ElectromagneticEmissionsPduRecord();
        emissions.setTimestamp(disEmissionsTime);
        electromagneticEmissionsEvents.add(emissions);
        
        // Mock repository calls
        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(entityStates);
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(fireEvents);
        when(collisionRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(collisionEvents);
        when(detonationRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(detonationEvents);
        when(dataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(dataPduEvents);
        when(actionRequestPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(actionRequestEvents);
        when(startResumePduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(startResumeEvents);
        when(setDataPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(setDataEvents);
        when(designatorPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(designatorEvents);
        when(electromagneticEmissionsPduRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(electromagneticEmissionsEvents);
        
        AggregatedMetricsOverview overview = metricsService.getAggregatedMetrics("last60minutes");
        
        // Verify counts for each PDU type
        assertEquals(10, overview.getTotalPackets());
        assertEquals(1, overview.getEntityStatePackets());
        assertEquals(1, overview.getFireEventPackets());
        assertEquals(1, overview.getCollisionPackets());
        assertEquals(1, overview.getDetonationPackets());
        assertEquals(1, overview.getDataPduPackets());
         assertEquals(1, overview.getActionRequestPduPackets());
         assertEquals(1, overview.getStartResumePduPackets());
         assertEquals(1, overview.getSetDataPduPackets());
         assertEquals(1, overview.getDesignatorPduPackets());
         assertEquals(1, overview.getElectromagneticEmissionsPduPackets());
        
        // Verify average packets per second
        double durationSeconds = 60 * 60; // 60 minutes in seconds
        assertEquals(10.0 / durationSeconds, overview.getAveragePacketsPerSecond(), 0.0001);
        
        // Verify peak load (we have 5 PDUs in the same minute)
         // The last 5 PDUs are all within the same minute bucket (nowEpochSeconds - 60 to nowEpochSeconds)
         assertEquals(5, overview.getPeakLoad().getPacketsInPeakInterval());
         assertEquals(5.0/60.0, overview.getPeakLoad().getPeakPacketsPerSecond(), 0.00001);
    }
}