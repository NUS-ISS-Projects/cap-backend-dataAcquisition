package com.cap.dataAcquisition.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
public class AggregatedMetricsOverview {
    private String timeWindowDescription;
    private Instant dataFromUtc;
    private Instant dataUntilUtc;
    private long totalPackets;
    private long entityStatePackets;
    private long fireEventPackets;
    private long collisionPackets;
    private long detonationPackets;
    private long dataPduPackets;
    private long actionRequestPduPackets;
    private long startResumePduPackets;
    private long setDataPduPackets;
    private long designatorPduPackets;
    private long electromagneticEmissionsPduPackets;
    private double averagePacketsPerSecond;
    private PeakLoadInfo peakLoad;
    
    public AggregatedMetricsOverview(String timeWindowDescription, Instant dataFromUtc, Instant dataUntilUtc,
                                   long totalPackets, long entityStatePackets, long fireEventPackets,
                                   long collisionPackets, long detonationPackets, double averagePacketsPerSecond,
                                   PeakLoadInfo peakLoad) {
        this.timeWindowDescription = timeWindowDescription;
        this.dataFromUtc = dataFromUtc;
        this.dataUntilUtc = dataUntilUtc;
        this.totalPackets = totalPackets;
        this.entityStatePackets = entityStatePackets;
        this.fireEventPackets = fireEventPackets;
        this.collisionPackets = collisionPackets;
        this.detonationPackets = detonationPackets;
        this.dataPduPackets = 0;
        this.actionRequestPduPackets = 0;
        this.startResumePduPackets = 0;
        this.setDataPduPackets = 0;
        this.designatorPduPackets = 0;
        this.electromagneticEmissionsPduPackets = 0;
        this.averagePacketsPerSecond = averagePacketsPerSecond;
        this.peakLoad = peakLoad;
    }
    
    public AggregatedMetricsOverview(String timeWindowDescription, Instant dataFromUtc, Instant dataUntilUtc,
                                   long totalPackets, long entityStatePackets, long fireEventPackets,
                                   long collisionPackets, long detonationPackets, long dataPduPackets,
                                   long actionRequestPduPackets, long startResumePduPackets, long setDataPduPackets,
                                   long designatorPduPackets, long electromagneticEmissionsPduPackets,
                                   double averagePacketsPerSecond, PeakLoadInfo peakLoad) {
        this.timeWindowDescription = timeWindowDescription;
        this.dataFromUtc = dataFromUtc;
        this.dataUntilUtc = dataUntilUtc;
        this.totalPackets = totalPackets;
        this.entityStatePackets = entityStatePackets;
        this.fireEventPackets = fireEventPackets;
        this.collisionPackets = collisionPackets;
        this.detonationPackets = detonationPackets;
        this.dataPduPackets = dataPduPackets;
        this.actionRequestPduPackets = actionRequestPduPackets;
        this.startResumePduPackets = startResumePduPackets;
        this.setDataPduPackets = setDataPduPackets;
        this.designatorPduPackets = designatorPduPackets;
        this.electromagneticEmissionsPduPackets = electromagneticEmissionsPduPackets;
        this.averagePacketsPerSecond = averagePacketsPerSecond;
        this.peakLoad = peakLoad;
    }

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