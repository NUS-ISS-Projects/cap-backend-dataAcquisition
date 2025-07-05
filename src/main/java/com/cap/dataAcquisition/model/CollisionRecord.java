package com.cap.dataAcquisition.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class CollisionRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private int issuingSite;
    private int issuingApplication;
    private int issuingEntity;

    private int collidingSite;
    private int collidingApplication;
    private int collidingEntity;

    @Column(columnDefinition = "BIGINT")
    private long timestamp;
}