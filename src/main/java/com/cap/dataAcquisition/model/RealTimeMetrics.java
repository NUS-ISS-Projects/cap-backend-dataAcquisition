package com.cap.dataAcquisition.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RealTimeMetrics {
    private long lastPduReceivedTimestamp;
    private long pduCountLastProcessingCycle;
    // Ensure fields match exactly with the DTO in DataIngestionService
}