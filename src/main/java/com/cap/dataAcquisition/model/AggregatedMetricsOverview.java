package com.cap.dataAcquisition.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggregatedMetricsOverview {
    private String timeWindowDescription;
    private Instant dataFromUtc;
    private Instant dataUntilUtc;
    private long totalPackets;
    private double averagePacketsPerSecond;
    private PeakLoadInfo peakLoad;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PeakLoadInfo {
        private double peakPacketsPerSecond;
        private Instant peakIntervalStartUtc;
        private Instant peakIntervalEndUtc;
        private long packetsInPeakInterval;
    }
}