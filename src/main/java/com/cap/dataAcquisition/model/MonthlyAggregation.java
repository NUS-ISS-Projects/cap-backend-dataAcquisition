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
    
    public MonthlyAggregation(int year, int month, long entityStateCount, long fireEventCount, 
                             long collisionCount, long detonationCount) {
        super(entityStateCount, fireEventCount, collisionCount, detonationCount);
        this.year = year;
        this.month = month;
    }
    
    public MonthlyAggregation(int year, int month, long entityStateCount, long fireEventCount, 
                             long collisionCount, long detonationCount,
                             long dataPduCount, long actionRequestPduCount,
                             long startResumePduCount, long setDataPduCount,
                             long designatorPduCount, long electromagneticEmissionsPduCount) {
        super(entityStateCount, fireEventCount, collisionCount, detonationCount,
             dataPduCount, actionRequestPduCount, startResumePduCount, setDataPduCount,
             designatorPduCount, electromagneticEmissionsPduCount);
        this.year = year;
        this.month = month;
    }
}