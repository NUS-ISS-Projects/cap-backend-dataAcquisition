package com.cap.dataAcquisition.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggregationResult {
    private long entityStatePduCount;
    private long fireEventPduCount;
    // You could add counts for other PDU types if they are stored
}