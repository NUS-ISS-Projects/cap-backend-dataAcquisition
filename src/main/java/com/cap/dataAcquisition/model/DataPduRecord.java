package com.cap.dataAcquisition.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class DataPduRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private int originatingSite;
    private int originatingApplication;
    private int originatingEntity;

    private int receivingSite;
    private int receivingApplication;
    private int receivingEntity;

    @Column(columnDefinition = "BIGINT")
    private long timestamp;
}