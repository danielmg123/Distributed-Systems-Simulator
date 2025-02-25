package com.dss.backend.controller;

import com.dss.backend.dto.AlgorithmDTO;
import com.dss.backend.service.AlgorithmService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/algorithms")
public class AlgorithmController {

    @Autowired
    private AlgorithmService algorithmService;

    @GetMapping
    public ResponseEntity<List<AlgorithmDTO>> getAvailableAlgorithms() {
        List<AlgorithmDTO> algorithms = algorithmService.getAvailableAlgorithms();
        return ResponseEntity.ok(algorithms);
    }
}