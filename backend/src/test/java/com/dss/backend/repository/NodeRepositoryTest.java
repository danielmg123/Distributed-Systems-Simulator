package com.dss.backend.repository;

import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DataMongoTest
public class NodeRepositoryTest {

    @Autowired
    private NodeRepository nodeRepository;

    @Test
    public void contextLoads() {
        assertNotNull(nodeRepository);
    }

    @Test
    public void testFindByStatus() {
        // Clean up the repository.
        nodeRepository.deleteAll();

        // Insert multiple nodes with different statuses.
        Node activeNode1 = new Node();
        activeNode1.setAddress("111 Active St");
        activeNode1.setStatus(NodeStatus.ACTIVE);
        nodeRepository.save(activeNode1);

        Node activeNode2 = new Node();
        activeNode2.setAddress("222 Active St");
        activeNode2.setStatus(NodeStatus.ACTIVE);
        nodeRepository.save(activeNode2);

        Node inactiveNode = new Node();
        inactiveNode.setAddress("333 Inactive St");
        inactiveNode.setStatus(NodeStatus.INACTIVE);
        nodeRepository.save(inactiveNode);

        // Query by ACTIVE status.
        List<Node> activeNodes = nodeRepository.findByStatus(NodeStatus.ACTIVE);
        assertNotNull(activeNodes);
        assertEquals(2, activeNodes.size());
    }

    @Test
    public void testFindByAddress() {
        // Clean up the repository.
        nodeRepository.deleteAll();

        // Insert nodes with different addresses.
        Node node1 = new Node();
        node1.setAddress("123 Test Ave");
        node1.setStatus(NodeStatus.ACTIVE);
        nodeRepository.save(node1);

        Node node2 = new Node();
        node2.setAddress("456 Sample Rd");
        node2.setStatus(NodeStatus.INACTIVE);
        nodeRepository.save(node2);

        // Retrieve the node by address.
        Node found = nodeRepository.findByAddress("123 Test Ave");
        assertNotNull(found);
        assertEquals("123 Test Ave", found.getAddress());
    }
}