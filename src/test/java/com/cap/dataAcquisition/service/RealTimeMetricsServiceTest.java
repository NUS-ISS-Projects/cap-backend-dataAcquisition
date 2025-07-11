package com.cap.dataAcquisition.service;

import com.cap.dataAcquisition.model.RealTimeMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RealTimeMetricsServiceTest {

    @Mock
    private RestTemplate restTemplate; // [cite: 72]

    @InjectMocks
    private RealTimeMetricsService realTimeMetricsService; // [cite: 70]

    private String testServiceUrl = "http://fake-ingestion-service:8080";
    private String fullMetricsUrl;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(realTimeMetricsService, "dataIngestionServiceUrl", testServiceUrl); // [cite: 71]
        fullMetricsUrl = testServiceUrl + "/internal/metrics/realtime"; // [cite: 74]
    }

    @Test
    void getLatestMetrics_success() {
        RealTimeMetrics expectedMetrics = new RealTimeMetrics(
            12345L, // lastPduReceivedTimestampMs
            100L,   // pdusInLastSixtySeconds
            1.66,   // averagePduRatePerSecondLastSixtySeconds
            40L,    // entityStatePdusInLastSixtySeconds
            30L,    // fireEventPdusInLastSixtySeconds
            20L,    // collisionPdusInLastSixtySeconds
            10L     // detonationPdusInLastSixtySeconds
        );
        when(restTemplate.getForObject(eq(fullMetricsUrl), eq(RealTimeMetrics.class))).thenReturn(expectedMetrics);

        RealTimeMetrics actualMetrics = realTimeMetricsService.getLatestMetrics();

        assertNotNull(actualMetrics);
        assertEquals(expectedMetrics.getLastPduReceivedTimestampMs(), actualMetrics.getLastPduReceivedTimestampMs());
        assertEquals(expectedMetrics.getPdusInLastSixtySeconds(), actualMetrics.getPdusInLastSixtySeconds());
        assertEquals(expectedMetrics.getEntityStatePdusInLastSixtySeconds(), actualMetrics.getEntityStatePdusInLastSixtySeconds());
        assertEquals(expectedMetrics.getFireEventPdusInLastSixtySeconds(), actualMetrics.getFireEventPdusInLastSixtySeconds());
        assertEquals(expectedMetrics.getCollisionPdusInLastSixtySeconds(), actualMetrics.getCollisionPdusInLastSixtySeconds());
        assertEquals(expectedMetrics.getDetonationPdusInLastSixtySeconds(), actualMetrics.getDetonationPdusInLastSixtySeconds());
    }

    @Test
    void getLatestMetrics_nullResponseFromService() {
        when(restTemplate.getForObject(eq(fullMetricsUrl), eq(RealTimeMetrics.class))).thenReturn(null);

        RealTimeMetrics actualMetrics = realTimeMetricsService.getLatestMetrics();

        assertNotNull(actualMetrics); // Fallback metrics are returned
        assertEquals(0L, actualMetrics.getLastPduReceivedTimestampMs());
        assertEquals(0L, actualMetrics.getPdusInLastSixtySeconds());
        assertEquals(0.0, actualMetrics.getAveragePduRatePerSecondLastSixtySeconds());
        assertEquals(0L, actualMetrics.getEntityStatePdusInLastSixtySeconds());
        assertEquals(0L, actualMetrics.getFireEventPdusInLastSixtySeconds());
        assertEquals(0L, actualMetrics.getCollisionPdusInLastSixtySeconds());
        assertEquals(0L, actualMetrics.getDetonationPdusInLastSixtySeconds());
    }

    @Test
    void getLatestMetrics_restClientException() {
        when(restTemplate.getForObject(eq(fullMetricsUrl), eq(RealTimeMetrics.class)))
            .thenThrow(new RestClientException("Connection failed"));

        RealTimeMetrics actualMetrics = realTimeMetricsService.getLatestMetrics();

        assertNotNull(actualMetrics); // Fallback metrics are returned
        assertEquals(0L, actualMetrics.getLastPduReceivedTimestampMs());
        assertEquals(0L, actualMetrics.getPdusInLastSixtySeconds());
        assertEquals(0.0, actualMetrics.getAveragePduRatePerSecondLastSixtySeconds());
        assertEquals(0L, actualMetrics.getEntityStatePdusInLastSixtySeconds());
        assertEquals(0L, actualMetrics.getFireEventPdusInLastSixtySeconds());
        assertEquals(0L, actualMetrics.getCollisionPdusInLastSixtySeconds());
        assertEquals(0L, actualMetrics.getDetonationPdusInLastSixtySeconds());
        // The fallback message is logged, not part of the returned DTO usually
    }
}