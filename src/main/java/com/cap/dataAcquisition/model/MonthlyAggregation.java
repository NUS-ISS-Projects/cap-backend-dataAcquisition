package com.cap.dataAcquisition.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class MonthlyAggregation extends AggregationResult {
    private int year;
    private int month;

    public MonthlyAggregation(int year, int month, long entityStateCount, long fireEventCount) {
        super(entityStateCount, fireEventCount);
        this.year = year;
        this.month = month;
    }
}