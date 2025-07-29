package com.cap.dataAcquisition.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TimeBucketAggregation {
    private String timeUnit; // "hour", "day", "week"
    private String start;
    private String end;
    private List<TimeBucket> buckets;
}