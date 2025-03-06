package com.cap.dataAcquisition.controller;

import com.cap.dataAcquisition.model.EntityStateRecord;
import com.cap.dataAcquisition.model.FireEventRecord;
import com.cap.dataAcquisition.repository.EntityStateRepository;
import com.cap.dataAcquisition.repository.FireEventRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/data-acquisition")
public class HistoricalDataController {

    @Autowired
    private EntityStateRepository entityStateRepository;

    @Autowired
    private FireEventRepository fireEventRepository;

    @GetMapping("/entity-states")
    public List<EntityStateRecord> getEntityStates(
            @RequestParam(required = false) Long startTime,
            @RequestParam(required = false) Long endTime) {
        if (startTime != null && endTime != null) {
            return entityStateRepository.findByTimestampBetween(startTime, endTime);
        }
        return entityStateRepository.findAll();
    }

    @GetMapping("/fire-events")
    public List<FireEventRecord> getFireEvents(
            @RequestParam(required = false) Long startTime,
            @RequestParam(required = false) Long endTime) {
        if (startTime != null && endTime != null) {
            return fireEventRepository.findByTimestampBetween(startTime, endTime);
        }
        return fireEventRepository.findAll();
    }
}
