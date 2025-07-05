package com.cap.dataAcquisition.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class AggregationResult {
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
    
    // Constructor for original PDU types
    public AggregationResult(long entityStatePduCount, long fireEventPduCount) {
        this.entityStatePduCount = entityStatePduCount;
        this.fireEventPduCount = fireEventPduCount;
        this.collisionPduCount = 0;
        this.detonationPduCount = 0;
        this.dataPduCount = 0;
        this.actionRequestPduCount = 0;
        this.startResumePduCount = 0;
        this.setDataPduCount = 0;
        this.designatorPduCount = 0;
        this.electromagneticEmissionsPduCount = 0;
    }
    
    // Constructor for original four PDU types
    public AggregationResult(long entityStatePduCount, long fireEventPduCount, 
                            long collisionPduCount, long detonationPduCount) {
        this.entityStatePduCount = entityStatePduCount;
        this.fireEventPduCount = fireEventPduCount;
        this.collisionPduCount = collisionPduCount;
        this.detonationPduCount = detonationPduCount;
        this.dataPduCount = 0;
        this.actionRequestPduCount = 0;
        this.startResumePduCount = 0;
        this.setDataPduCount = 0;
        this.designatorPduCount = 0;
        this.electromagneticEmissionsPduCount = 0;
    }
    
    // Full constructor with all PDU types
    public AggregationResult(long entityStatePduCount, long fireEventPduCount, 
                            long collisionPduCount, long detonationPduCount,
                            long dataPduCount, long actionRequestPduCount,
                            long startResumePduCount, long setDataPduCount,
                            long designatorPduCount, long electromagneticEmissionsPduCount) {
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
    }
}