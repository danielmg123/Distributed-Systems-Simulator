package com.dss.backend.service;

import com.dss.backend.dto.AlgorithmDTO;
import com.dss.backend.model.ConsensusAlgorithmType;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

/**
 * <p>
 * The {@code AlgorithmService} provides a way to list all available
 * consensus algorithms that the simulator can use.
 * </p>
 *
 * <p>
 * <strong>Business Logic:</strong>
 * <ul>
 *   <li>Defines a hard-coded set of supported algorithms (Paxos, Raft, Multi-Paxos, etc.).</li>
 *   <li>Returns them to controllers or any other caller that needs to display or
 *       select the consensus algorithms available in the system.</li>
 * </ul>
 * </p>
 *
 * <p>
 * <strong>Side Effects:</strong>
 * <ul>
 *   <li>This service does not modify the database or the application state. It simply
 *       exposes a fixed list of algorithms that can be displayed in a UI or used to validate
 *       user choices during simulation setup.</li>
 * </ul>
 * </p>
 */
@Service
public class AlgorithmService {

    /**
     * <p>Retrieves a list of all available consensus algorithms along with their names
     * and enum types.</p>
     *
     * <p>
     * This is used by the front end or other layers to present the user with a
     * selection of possible consensus approaches (e.g. Paxos, Raft).
     * </p>
     *
     * @return a list of {@link AlgorithmDTO} objects describing each supported algorithm.
     */
    public List<AlgorithmDTO> getAvailableAlgorithms() {
        // Hard-coded array of known supported algorithms
        return Arrays.asList(
                new AlgorithmDTO("Paxos", ConsensusAlgorithmType.PAXOS),
                new AlgorithmDTO("Raft", ConsensusAlgorithmType.RAFT),
                new AlgorithmDTO("Multi-Paxos", ConsensusAlgorithmType.MULTI_PAXOS)
        );
    }
}