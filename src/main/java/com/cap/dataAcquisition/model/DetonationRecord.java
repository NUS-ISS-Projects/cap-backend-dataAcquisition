package com.cap.dataAcquisition.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class DetonationRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private int firingSite;
    private int firingApplication;
    private int firingEntity;

    private int targetSite;
    private int targetApplication;
    private int targetEntity;

    private double locationX;
    private double locationY;
    private double locationZ;

    @Column(columnDefinition = "BIGINT")
    private long timestamp;
}