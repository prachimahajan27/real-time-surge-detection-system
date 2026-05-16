package com.example.surge.model;

import lombok.AllArgsConstructor;

import lombok.Data;

import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SurgeEvent {

    private String zoneId;

    private Long requestCount;

    private Double surgeMultiplier;

    private String windowStart;

    private String windowEnd;

}