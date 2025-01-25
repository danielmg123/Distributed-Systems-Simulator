package com.dss.backend.repository;

import com.dss.backend.model.Node;
import com.dss.backend.model.NodeStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringJUnitConfig
@DataMongoTest
public class NodeRepositoryTest {

    @Autowired
    private NodeRepository nodeRepository;

    @Test
    public void contextLoads() throws Exception {
        assertNotNull(nodeRepository);
    }

    @Test
    public void testFindByStatus() {
        Node testNode = new Node();
        testNode.setAddress("123 Test Street");
        testNode.setStatus(NodeStatus.ACTIVE);
        nodeRepository.save(testNode);

        assertNotNull(nodeRepository.findByStatus(NodeStatus.ACTIVE));
    }
}
