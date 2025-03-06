package com.cap.dataAcquisition.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class FireEventRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private int firingSite;
    private int firingApplication;
    private int firingEntity;

    private int targetSite;
    private int targetApplication;
    private int targetEntity;

    private int munitionSite;
    private int munitionApplication;
    private int munitionEntity;

    private long timestamp;
}
