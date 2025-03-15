package com.dss.backend.controller;

import com.dss.backend.dto.AlgorithmDTO;
import com.dss.backend.service.AlgorithmService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/algorithms")
@Tag(name = "Algorithm Controller", description = "Endpoints for retrieving consensus algorithms")
public class AlgorithmController {

    @Autowired
    private AlgorithmService algorithmService;

    @Operation(
            summary = "Get Available Algorithms",
            description = "Retrieves a list of available consensus algorithms for the simulator."
    )
    @GetMapping
    public ResponseEntity<List<AlgorithmDTO>> getAvailableAlgorithms() {
        List<AlgorithmDTO> algorithms = algorithmService.getAvailableAlgorithms();
        return ResponseEntity.ok(algorithms);
    }
}