package com.cap.dataAcquisition.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class EntityStateRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private int site;
    private int application;
    private int entity;

    private double locationX;
    private double locationY;
    private double locationZ;

    private long timestamp;
}
