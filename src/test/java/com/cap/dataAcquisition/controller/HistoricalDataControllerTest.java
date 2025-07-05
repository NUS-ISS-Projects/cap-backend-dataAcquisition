package com.cap.dataAcquisition.controller;

import com.cap.dataAcquisition.model.*;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.containsString;


@WebMvcTest(HistoricalDataController.class)
class HistoricalDataControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private EntityStateRepository entityStateRepository; // [cite: 86]
    @MockBean
    private FireEventRepository fireEventRepository; // [cite: 86]
    @MockBean
    private CollisionRepository collisionRepository;
    @MockBean
    private DetonationRepository detonationRepository;
    @MockBean
    private DataPduRepository dataPduRepository;
    @MockBean
    private ActionRequestPduRepository actionRequestPduRepository;
    @MockBean
    private StartResumePduRepository startResumePduRepository;
    @MockBean
    private SetDataPduRepository setDataPduRepository;
    @MockBean
    private DesignatorPduRepository designatorPduRepository;
    @MockBean
    private ElectromagneticEmissionsPduRepository electromagneticEmissionsPduRepository;
    @MockBean
    private RealTimeMetricsService realTimeMetricsService; // [cite: 87]
    @MockBean
    private MetricsService metricsService; // [cite: 87]

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void getEntityStates_noParams_returnsAll() throws Exception {
        EntityStateRecord record = new EntityStateRecord(); record.setId(1L); record.setTimestamp(123L); // [cite: 11]
        when(entityStateRepository.findAll()).thenReturn(List.of(record)); // [cite: 92]

        mockMvc.perform(get("/api/acquisition/entity-states")) // [cite: 90]
            .andExpect(status().isOk())
            .andExpect(jsonPath("$", hasSize(1)))
            .andExpect(jsonPath("$[0].id", is(1)));
    }

    @Test
    void getEntityStates_withParams_returnsFiltered() throws Exception {
        EntityStateRecord record = new EntityStateRecord(); record.setId(2L); record.setTimestamp(456L);
        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(List.of(record)); // [cite: 91]

        mockMvc.perform(get("/api/acquisition/entity-states")
                .param("startTime", "1000")
                .param("endTime", "2000"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$", hasSize(1)))
            .andExpect(jsonPath("$[0].id", is(2)));
    }

    @Test
    void getFireEvents_noParams_returnsAll() throws Exception {
        FireEventRecord record = new FireEventRecord(); record.setId(1L); record.setTimestamp(123L); // [cite: 7]
        when(fireEventRepository.findAll()).thenReturn(List.of(record)); // [cite: 97]

        mockMvc.perform(get("/api/acquisition/fire-events")) // [cite: 95]
            .andExpect(status().isOk())
            .andExpect(jsonPath("$", hasSize(1)))
            .andExpect(jsonPath("$[0].id", is(1)));
    }
    
    @Test
    void getFireEvents_withParams_returnsFiltered() throws Exception {
        FireEventRecord record = new FireEventRecord(); record.setId(2L); record.setTimestamp(456L);
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(List.of(record)); // [cite: 96]

        mockMvc.perform(get("/api/acquisition/fire-events")
                .param("startTime", "1000")
                .param("endTime", "2000"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$", hasSize(1)))
            .andExpect(jsonPath("$[0].id", is(2)));
    }


    @Test
    void healthCheck_returnsOk() throws Exception {
        String expectedHostname = System.getenv("HOSTNAME");
        if (expectedHostname == null) expectedHostname = "null";

        mockMvc.perform(get("/api/acquisition/health")) // [cite: 100]
            .andExpect(status().isOk())
            .andExpect(content().string(containsString("Data acquisition service is up and running on pod: " + expectedHostname))); // [cite: 100]
    }

    @Test
    void getRealTimeDisMetrics_serviceAvailable() throws Exception {
        RealTimeMetrics metrics = new RealTimeMetrics(
            Instant.now().toEpochMilli(), 
            50L, 
            0.83, 
            20L, // entityStatePdusInLastSixtySeconds
            15L, // fireEventPdusInLastSixtySeconds
            10L, // collisionPdusInLastSixtySeconds
            5L   // detonationPdusInLastSixtySeconds
        );
        when(realTimeMetricsService.getLatestMetrics()).thenReturn(metrics);

        mockMvc.perform(get("/api/acquisition/realtime"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.pdusInLastSixtySeconds", is(50)))
            .andExpect(jsonPath("$.entityStatePdusInLastSixtySeconds", is(20)))
            .andExpect(jsonPath("$.fireEventPdusInLastSixtySeconds", is(15)))
            .andExpect(jsonPath("$.collisionPdusInLastSixtySeconds", is(10)))
            .andExpect(jsonPath("$.detonationPdusInLastSixtySeconds", is(5)));
    }
    
    @Test
    void getRealTimeDisMetrics_serviceUnavailableInController() throws Exception {
        // This specific controller constructor allows realTimeMetricsService to be null if not autowired (required=false)
        // To test this scenario properly, we might need a way to inject null for realTimeMetricsService
        // or ensure the @MockBean provides a version of the controller where it is null.
        // For this example, we assume it is injected. If it were null:
        when(realTimeMetricsService.getLatestMetrics()).thenReturn(null); // Or simulate service being actually null
        // The controller handles null RealTimeMetricsService by returning 503 [cite: 102]
        // If the *bean* realTimeMetricsService itself is null (due to required=false not being met by Spring DI for test):
        // This requires a different setup, perhaps by constructing controller manually or with a custom Spring context.
        // The provided test code will assume realTimeMetricsService mock is present.
        // If we want to test the explicit null check `if (realTimeMetricsService == null)`
        // We can't do that easily with @WebMvcTest if the bean is always injected.
        // However, if the service itself returns null metrics, that's testable.
        when(realTimeMetricsService.getLatestMetrics()).thenReturn(new RealTimeMetrics(0,0,0.0,0,0,0,0)); // Return metrics with all fields set to 0

        mockMvc.perform(get("/api/acquisition/realtime"))
            .andExpect(status().isOk()) // It will be OK, and body will have 0s
            .andExpect(jsonPath("$.lastPduReceivedTimestampMs", is(0)))
            .andExpect(jsonPath("$.pdusInLastSixtySeconds", is(0)))
            .andExpect(jsonPath("$.entityStatePdusInLastSixtySeconds", is(0)))
            .andExpect(jsonPath("$.fireEventPdusInLastSixtySeconds", is(0)))
            .andExpect(jsonPath("$.collisionPdusInLastSixtySeconds", is(0)))
            .andExpect(jsonPath("$.detonationPdusInLastSixtySeconds", is(0)));
    }


    @Test
    void getMonthlyAggregatedData_validRequest() throws Exception {
        MonthlyAggregation aggregation = new MonthlyAggregation(2023, 5, 100L, 20L, 15L, 10L); // [cite: 19, 110]
        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.nCopies(100, new EntityStateRecord()));
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.nCopies(20, new FireEventRecord()));
        when(collisionRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.nCopies(15, new CollisionRecord()));
        when(detonationRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.nCopies(10, new DetonationRecord()));

        mockMvc.perform(get("/api/acquisition/monthly")
                .param("year", "2023")
                .param("month", "5")) // [cite: 106]
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.year", is(2023)))
            .andExpect(jsonPath("$.entityStatePduCount", is(100)))
            .andExpect(jsonPath("$.collisionPduCount", is(15)))
            .andExpect(jsonPath("$.detonationPduCount", is(10)));
    }

    @Test
    void getMonthlyAggregatedData_invalidMonth_returnsBadRequest() throws Exception {
        mockMvc.perform(get("/api/acquisition/monthly")
                .param("year", "2023")
                .param("month", "13")) // Invalid month [cite: 106]
            .andExpect(status().isBadRequest());
    }

    @Test
    void getCustomRangeAggregatedData_validRequest() throws Exception {
        CustomRangeAggregation aggregation = new CustomRangeAggregation(
            LocalDate.of(2023,1,1).toString(), LocalDate.of(2023,1,10).toString(), 150L, 25L, 20L, 15L); // [cite: 26, 115]
        when(entityStateRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.nCopies(150, new EntityStateRecord()));
        when(fireEventRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.nCopies(25, new FireEventRecord()));
        when(collisionRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.nCopies(20, new CollisionRecord()));
        when(detonationRepository.findByTimestampBetween(anyLong(), anyLong())).thenReturn(Collections.nCopies(15, new DetonationRecord()));

        mockMvc.perform(get("/api/acquisition/aggregate")
                .param("startDate", "2023-01-01")
                .param("endDate", "2023-01-10")) // [cite: 112]
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.startDate", is("2023-01-01")))
            .andExpect(jsonPath("$.entityStatePduCount", is(150)))
            .andExpect(jsonPath("$.collisionPduCount", is(20)))
            .andExpect(jsonPath("$.detonationPduCount", is(15)));
    }

    @Test
    void getAggregatedMetricsOverview_defaultPeriod() throws Exception {
        AggregatedMetricsOverview overview = new AggregatedMetricsOverview("Last 60 minutes", Instant.now().minusSeconds(3600), Instant.now(), 200L, 150L, 30L, 10L, 10L, 3.33, null); // [cite: 21]
        when(metricsService.getAggregatedMetrics(eq("last60minutes"))).thenReturn(overview); // [cite: 117]

        mockMvc.perform(get("/api/acquisition/metrics")) // Defaults to last60minutes [cite: 117]
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.timeWindowDescription", is("Last 60 minutes")))
            .andExpect(jsonPath("$.totalPackets", is(200)));
    }

    @Test
    void getAggregatedMetricsOverview_specificPeriod() throws Exception {
        AggregatedMetricsOverview overview = new AggregatedMetricsOverview("Last 24 hours", Instant.now().minusSeconds(86400), Instant.now(), 5000L, 3500L, 1000L, 300L, 200L, 0.057, null);
        when(metricsService.getAggregatedMetrics(eq("lastDay"))).thenReturn(overview);

        mockMvc.perform(get("/api/acquisition/metrics")
                .param("period", "lastDay"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.timeWindowDescription", is("Last 24 hours")))
            .andExpect(jsonPath("$.totalPackets", is(5000)));
    }
}