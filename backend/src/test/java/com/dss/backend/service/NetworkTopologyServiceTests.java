package com.dss.backend.service;

import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.model.NetworkTopology;
import com.dss.backend.model.TopologyType;
import com.dss.backend.repository.NetworkTopologyRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class NetworkTopologyServiceTests {

    @Mock
    private NetworkTopologyRepository networkTopologyRepository;

    @InjectMocks
    private NetworkTopologyService networkTopologyService;

    private NetworkTopology topology;

    @BeforeEach
    public void setup() {
        topology = new NetworkTopology();
        topology.setId("top1");
        topology.setType(TopologyType.MESH);
    }

    @Test
    public void getAllTopologies_ReturnsAll() {
        List<NetworkTopology> topologies = Arrays.asList(topology);
        when(networkTopologyRepository.findAll()).thenReturn(topologies);

        List<NetworkTopology> result = networkTopologyService.getAllTopologies();
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void getTopologyByIdOrThrow_ValidId_ReturnsTopology() {
        when(networkTopologyRepository.findById("top1")).thenReturn(Optional.of(topology));
        NetworkTopology result = networkTopologyService.getTopologyByIdOrThrow("top1");
        assertNotNull(result);
        assertEquals("top1", result.getId());
    }

    @Test
    public void getTopologyByIdOrThrow_InvalidId_ThrowsException() {
        when(networkTopologyRepository.findById("invalid")).thenReturn(Optional.empty());
        Exception exception = assertThrows(ResourceNotFoundException.class, () -> {
            networkTopologyService.getTopologyByIdOrThrow("invalid");
        });
        assertTrue(exception.getMessage().contains("NetworkTopology not found with id: invalid"));
    }

    @Test
    public void saveTopology_ValidObject_SavesAndReturns() {
        when(networkTopologyRepository.save(topology)).thenReturn(topology);
        NetworkTopology saved = networkTopologyService.saveTopology(topology);
        assertNotNull(saved);
        assertEquals("top1", saved.getId());
        verify(networkTopologyRepository, times(1)).save(topology);
    }

    @Test
    public void deleteTopology_ValidId_DeletesTopology() {
        when(networkTopologyRepository.findById("top1")).thenReturn(Optional.of(topology));
        doNothing().when(networkTopologyRepository).deleteById("top1");

        networkTopologyService.deleteTopology("top1");
        verify(networkTopologyRepository, times(1)).deleteById("top1");
    }
}