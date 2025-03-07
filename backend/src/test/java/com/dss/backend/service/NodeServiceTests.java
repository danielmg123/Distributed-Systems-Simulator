package com.dss.backend.service;

import com.dss.backend.exception.ResourceNotFoundException;
import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import com.dss.backend.repository.NodeRepository;
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
public class NodeServiceTests {

    @Mock
    private NodeRepository nodeRepository;

    @InjectMocks
    private NodeService nodeService;

    private Node node1;
    private Node node2;

    @BeforeEach
    public void setup() {
        node1 = new Node();
        node1.setId("id1");
        node1.setAddress("address1");
        node1.setStatus(NodeStatus.ACTIVE);

        node2 = new Node();
        node2.setId("id2");
        node2.setAddress("address2");
        node2.setStatus(NodeStatus.ACTIVE);
    }

    @Test
    public void getAllNodes_ReturnsNodesFromRepository() {
        List<Node> nodes = Arrays.asList(node1, node2);
        when(nodeRepository.findAll()).thenReturn(nodes);

        List<Node> result = nodeService.getAllNodes();
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("id1", result.get(0).getId());
    }

    @Test
    public void getNodeByIdOrThrow_ValidId_ReturnsNode() {
        when(nodeRepository.findById("id1")).thenReturn(Optional.of(node1));

        Node result = nodeService.getNodeByIdOrThrow("id1");
        assertNotNull(result);
        assertEquals("id1", result.getId());
    }

    @Test
    public void getNodeByIdOrThrow_InvalidId_ThrowsResourceNotFound() {
        when(nodeRepository.findById("invalid")).thenReturn(Optional.empty());

        Exception exception = assertThrows(ResourceNotFoundException.class, () -> {
            nodeService.getNodeByIdOrThrow("invalid");
        });
        String expectedMessage = "Node not found with id: invalid";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    public void saveNode_ValidNode_SavesToRepository() {
        when(nodeRepository.save(node1)).thenReturn(node1);

        Node saved = nodeService.saveNode(node1);
        assertNotNull(saved);
        assertEquals("id1", saved.getId());
        verify(nodeRepository, times(1)).save(node1);
    }

    @Test
    public void deleteNode_ValidId_DeletesFromRepository() {
        when(nodeRepository.findById("id1")).thenReturn(Optional.of(node1));
        doNothing().when(nodeRepository).deleteById("id1");

        nodeService.deleteNode("id1");
        verify(nodeRepository, times(1)).deleteById("id1");
    }
}