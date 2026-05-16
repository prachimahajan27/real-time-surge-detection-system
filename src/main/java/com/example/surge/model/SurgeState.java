package com.example.surge.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SurgeState {

    private Double lastMultiplier;

    private Long lastSurgeTimestamp;



}
