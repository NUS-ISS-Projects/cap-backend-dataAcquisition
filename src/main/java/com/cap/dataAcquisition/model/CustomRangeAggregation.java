package com.cap.dataAcquisition.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class CustomRangeAggregation extends AggregationResult {
    private String startDate; // Or use Instant/OffsetDateTime for better type safety
    private String endDate;

    public CustomRangeAggregation(String startDate, String endDate, long entityStateCount, long fireEventCount) {
        super(entityStateCount, fireEventCount);
        this.startDate = startDate;
        this.endDate = endDate;
    }
}