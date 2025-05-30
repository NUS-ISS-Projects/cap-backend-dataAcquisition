package com.cap.dataAcquisition.service;

import com.cap.dataAcquisition.model.RealTimeMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Autowired;

@Service
public class RealTimeMetricsService {

    private static final Logger log = LoggerFactory.getLogger(RealTimeMetricsService.class);

    private final RestTemplate restTemplate;

    @Value("${metrics.dataIngestion.service.url}") // Configure this in application.properties
    private String dataIngestionServiceUrl;

    @Autowired
    public RealTimeMetricsService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public RealTimeMetrics getLatestMetrics() {
        String fullMetricsUrl = dataIngestionServiceUrl + "/internal/metrics/realtime"; // Path matches DataIngestionService
        try {
            log.debug("Fetching real-time metrics from: {}", fullMetricsUrl);
            RealTimeMetrics metrics = restTemplate.getForObject(fullMetricsUrl, RealTimeMetrics.class);
            if (metrics == null) {
                log.warn("Received null metrics from {}", fullMetricsUrl);
                return createFallbackMetrics("Null response from metrics service at " + fullMetricsUrl);
            }
            return metrics;
        } catch (RestClientException e) {
            log.error("Error fetching metrics from {}: {}", fullMetricsUrl, e.getMessage(), e);
            return createFallbackMetrics("Failed to connect to metrics service at " + fullMetricsUrl + ": " + e.getMessage());
        }
    }

    private RealTimeMetrics createFallbackMetrics(String errorMessage) {
        // You can decide how to handle errors, e.g., return empty/default metrics or throw
        log.warn("Falling back to default metrics. Error: {}", errorMessage);
        return new RealTimeMetrics(0L, 0L); // Default/empty metrics
    }
}