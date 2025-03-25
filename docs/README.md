# Distributed Systems Simulator (DSS)

The **Distributed Systems Simulator (DSS)** is a comprehensive framework designed to model and experiment with the core principles of distributed consensus. It provides a simulated environment to test, visualize, and compare multiple consensus algorithms such as Paxos, Multi-Paxos, Raft, ZooKeeper Atomic Broadcast (Zab), and View-Stamped Replication (VSR).

## Introduction to Distributed Systems

A distributed system is a network of independent computers that work together as a single system to achieve a common goal. Key challenges include:
- **Consensus and Coordination:** Ensuring that multiple nodes agree on a common state despite failures or delays.
- **Fault Tolerance:** Detecting and recovering from node failures without compromising system integrity.
- **Scalability and Heterogeneity:** Handling growing numbers of nodes and diverse network conditions.

In DSS, these challenges are explored by simulating how nodes interact using different consensus protocols while accounting for network topologies, message routing, and node failures.

## Project Purpose

The primary objectives of DSS are:
- **Experimentation:** To serve as a testbed for comparing consensus algorithms under controlled conditions.
- **Education:** To provide detailed insights into how consensus is achieved in distributed systems.
- **Simulation:** To mimic real-world distributed behavior using virtual nodes, a centralized message router, and schedulers for periodic tasks such as heartbeats and metrics collection.

By simulating these aspects, DSS helps researchers and developers understand both the strengths and limitations of various distributed consensus approaches.

## What is Implemented

DSS includes the following core components and features:

- **Consensus Algorithms:**  
  - **Paxos & Multi-Paxos:** Implements basic Paxos along with an optimized Multi-Paxos variant where a leader reduces the overhead of repeated prepare phases.
  - **Raft:** Provides leader election, log replication, and safety guarantees with clear separation between leader and follower roles.
  - **ZooKeeper Atomic Broadcast (Zab):** Models a single-leader protocol for strict ordering and quorum-based commit decisions.
  - **View-Stamped Replication (VSR):** Uses a primary-backup model with a three-phase protocol (PREPARE, PREPARE_RESPONSE, COMMIT).

- **Virtual Node and Messaging Infrastructure:**  
  - **VirtualNode:** Each simulated node wraps a consensus algorithm and handles message queuing, processing, and heartbeat management.
  - **MessageRouter:** A central messaging hub that decouples nodes from one another, allowing simulated message delays, logging, and routing.
  - **Scheduler and Heartbeat Services:** Use configurable thread pools to schedule recurring tasks (e.g., heartbeats, consensus timeouts, and metrics updates).

- **Simulation Orchestration and Metrics:**  
  - **Simulation Engine/Orchestrator:** Coordinates node initialization, simulation start/stop, failure injection, and real-time event logging.
  - **Metrics Collection:** Aggregates performance metrics (latency, throughput, failure recovery) and pushes snapshots to a front-end dashboard via WebSockets.

- **Deployment and Integration:**  
  - **Database Integration:** Uses MongoDB for persisting simulation data, nodes, and topologies.
  - **Containerization:** Provides Docker Compose and Kubernetes manifests to support local development and scalable cloud deployments.
  - **Security:** Implements optional JWT-based authentication (configurable for development vs. production).

## What is Not Fully Implemented

While DSS simulates many core aspects of distributed systems, certain production-level features are simplified or left as potential enhancements:

- **Dynamic Membership and Reconfiguration:**  
  - The current design assumes a fixed set of nodes during a simulation run. Dynamic addition or removal of nodes is not supported.

- **Robust Network Partitioning and Recovery:**  
  - Although node failures and heartbeat detection are simulated, advanced handling of network partitions or split-brain scenarios is not implemented.

- **Persistent Storage for Logs and State:**  
  - The consensus algorithms maintain in-memory state for logs and proposals. Durable storage mechanisms (such as disk-based persistence) are abstracted away for simulation purposes.

- **Advanced Leader Election and Conflict Resolution:**  
  - Leader election in protocols like Raft is demonstrated in a simplified manner without advanced randomization or tie-breaking strategies.

## Project Structure

The codebase is organized into several top-level directories:

- **backend/**  
  Contains the core Spring Boot application with packages for configuration, consensus algorithms, messaging, simulation engine, security, and REST controllers.

- **database/**  
  Includes the MongoDB initialization script (`init.sql`).

- **deployment/**  
  Provides Docker Compose and Kubernetes YAML files for deploying the backend and its dependencies.

- **docs/**  
  Contains detailed documentation on architecture, sequence diagrams, the simulation lifecycle, and additional design decisions:
  - [consensus-architecture.md](docs/consensus-architecture.md)
  - [consensus-sequence-diagrams.md](docs/consensus-sequence-diagrams.md)
  - [high-level-architecture.md](docs/high-level-architecture.md)
  - [simulation-lifecycle.md](docs/simulation-lifecycle.md)
  - [simulation-node-state.md](docs/simulation-node-state.md)

- **frontend/**  
  (Not covered in this document)

- **tests/**  
  Comprehensive unit, integration, and performance tests to validate the functionality of each module.

## Conclusion

The Distributed Systems Simulator provides a rich environment for studying how consensus protocols function within distributed systems. By abstracting node interactions through virtual nodes and a central message router, DSS makes it easier to understand the trade-offs, behaviors, and failure modes of key consensus algorithms. For a deeper dive into the design details, please refer to the documentation files in the docs folder.

---

For additional details and architectural insights, see our documentation:
- [Consensus Architecture](docs/consensus-architecture.md)
- [Sequence Diagrams](docs/consensus-sequence-diagrams.md)
- [High-Level Architecture](docs/high-level-architecture.md)
- [Simulation Lifecycle](docs/simulation-lifecycle.md)
- [Node and Simulation State](docs/simulation-node-state.md)