# HyDFS - Highly Distributed File System

HyDFS is a **Highly Distributed File System** designed to provide scalable, reliable, and efficient file storage and access across multiple nodes. It leverages consistent hashing for data distribution, ensures data replication for fault tolerance, and incorporates client-side caching to optimize read performance. HyDFS supports concurrent operations, including appends and merges, while maintaining data consistency across replicas.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Contact](#contact)

---

## Features

- **Data Replication:** Ensures high availability and fault tolerance by replicating each file across multiple server nodes.
- **Consistent Hashing:** Distributes files evenly across servers, facilitating scalability and efficient load balancing.
- **Client-Side Caching:** Reduces read latency by caching frequently accessed files on the client side.
- **Concurrent Operations:** Supports multiple clients performing concurrent append and merge operations on the same file.
- **Failure Detection:** Monitors server health through heartbeat mechanisms and handles replication after failures.
- **Logging:** Maintains detailed logs for server activities, aiding in monitoring and debugging.

## Architecture

HyDFS follows a masterless peer-to-peer architecture where each server node is responsible for a portion of the data based on consistent hashing. The system comprises two main components:

1. **HyDFS Server:**
   - Manages file storage, replication, and handles client requests.
   - Maintains a portion of the consistent hash ring and is responsible for specific file replicas.
   - Detects server failures and ensures data replication to maintain the desired replication factor.

2. **HyDFS Client:**
   - Provides an interface for users to perform file operations such as create, get, append, and merge.
   - Manages client-side caching to optimize read performance.
   - Handles redirection to primary servers and manages concurrent append operations.

## Prerequisites

Before setting up HyDFS, ensure that your system meets the following requirements:

- **Operating System:** Linux, macOS, or Windows
- **Go Version:** Go 1.16 or higher
- **Network:** Ensure that the necessary ports are open and accessible between server nodes and clients.

## Installation

### 1. Clone the Repository

```bash
git clone https://gitlab.engr.illinois.edu/alexy3/g07.git
cd mp3
```

### 2. Start Server
```bash
cd server
go run server.go
```

### 3. Start Client (Usage Displayed)
```bash
cd client
go run client.go '<server address>'
```

## Contact
Name: Claire Chen, Alex Yang
Email: cychen6@illinois.edu, alexy3@illinois.edu
