package com.cap.dataAcquisition.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RealTimeMetrics {
    private long lastPduReceivedTimestampMs;
    private long pdusInLastSixtySeconds;
    private double averagePduRatePerSecondLastSixtySeconds;
    // Ensure this structure matches the DTO in DataIngestionService
}