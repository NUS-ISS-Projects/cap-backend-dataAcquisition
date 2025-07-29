package com.cap.dataAcquisition.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TimeBucket {
    // Time identifier fields (only one will be used based on timeUnit)
    private Integer hour; // 0-23 for hourly buckets
    private String date; // YYYY-MM-DD for daily buckets
    private String week; // Week identifier for weekly buckets
    
    // PDU counts
    private long totalPackets;
    private long entityStatePduCount;
    private long fireEventPduCount;
    private long collisionPduCount;
    private long detonationPduCount;
    private long dataPduCount;
    private long actionRequestPduCount;
    private long startResumePduCount;
    private long setDataPduCount;
    private long designatorPduCount;
    private long electromagneticEmissionsPduCount;
    
    // Constructor for hourly buckets
    public TimeBucket(int hour, long entityStatePduCount, long fireEventPduCount, 
                     long collisionPduCount, long detonationPduCount,
                     long dataPduCount, long actionRequestPduCount,
                     long startResumePduCount, long setDataPduCount,
                     long designatorPduCount, long electromagneticEmissionsPduCount) {
        this.hour = hour;
        this.entityStatePduCount = entityStatePduCount;
        this.fireEventPduCount = fireEventPduCount;
        this.collisionPduCount = collisionPduCount;
        this.detonationPduCount = detonationPduCount;
        this.dataPduCount = dataPduCount;
        this.actionRequestPduCount = actionRequestPduCount;
        this.startResumePduCount = startResumePduCount;
        this.setDataPduCount = setDataPduCount;
        this.designatorPduCount = designatorPduCount;
        this.electromagneticEmissionsPduCount = electromagneticEmissionsPduCount;
        this.totalPackets = entityStatePduCount + fireEventPduCount + collisionPduCount + 
                           detonationPduCount + dataPduCount + actionRequestPduCount +
                           startResumePduCount + setDataPduCount + designatorPduCount +
                           electromagneticEmissionsPduCount;
    }
    
    // Constructor for daily buckets
    public TimeBucket(String date, long entityStatePduCount, long fireEventPduCount, 
                     long collisionPduCount, long detonationPduCount,
                     long dataPduCount, long actionRequestPduCount,
                     long startResumePduCount, long setDataPduCount,
                     long designatorPduCount, long electromagneticEmissionsPduCount) {
        this.date = date;
        this.entityStatePduCount = entityStatePduCount;
        this.fireEventPduCount = fireEventPduCount;
        this.collisionPduCount = collisionPduCount;
        this.detonationPduCount = detonationPduCount;
        this.dataPduCount = dataPduCount;
        this.actionRequestPduCount = actionRequestPduCount;
        this.startResumePduCount = startResumePduCount;
        this.setDataPduCount = setDataPduCount;
        this.designatorPduCount = designatorPduCount;
        this.electromagneticEmissionsPduCount = electromagneticEmissionsPduCount;
        this.totalPackets = entityStatePduCount + fireEventPduCount + collisionPduCount + 
                           detonationPduCount + dataPduCount + actionRequestPduCount +
                           startResumePduCount + setDataPduCount + designatorPduCount +
                           electromagneticEmissionsPduCount;
    }
    
    // Constructor for weekly buckets
    public TimeBucket(String week, boolean isWeekly, long entityStatePduCount, long fireEventPduCount, 
                     long collisionPduCount, long detonationPduCount,
                     long dataPduCount, long actionRequestPduCount,
                     long startResumePduCount, long setDataPduCount,
                     long designatorPduCount, long electromagneticEmissionsPduCount) {
        this.week = week;
        this.entityStatePduCount = entityStatePduCount;
        this.fireEventPduCount = fireEventPduCount;
        this.collisionPduCount = collisionPduCount;
        this.detonationPduCount = detonationPduCount;
        this.dataPduCount = dataPduCount;
        this.actionRequestPduCount = actionRequestPduCount;
        this.startResumePduCount = startResumePduCount;
        this.setDataPduCount = setDataPduCount;
        this.designatorPduCount = designatorPduCount;
        this.electromagneticEmissionsPduCount = electromagneticEmissionsPduCount;
        this.totalPackets = entityStatePduCount + fireEventPduCount + collisionPduCount + 
                           detonationPduCount + dataPduCount + actionRequestPduCount +
                           startResumePduCount + setDataPduCount + designatorPduCount +
                           electromagneticEmissionsPduCount;
    }
}