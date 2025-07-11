package com.cap.dataAcquisition.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PduLogResponse {
    
    @JsonProperty("Pdu_messages")
    private List<PduLogEntry> pduMessages;
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PduLogEntry {
        @JsonProperty("Id")
        private Long id;
        
        @JsonProperty("PDUType")
        private String pduType;
        
        @JsonProperty("length")
        private int length;
        
        @JsonProperty("recordDetails")
        private Map<String, Object> recordDetails;
    }
}