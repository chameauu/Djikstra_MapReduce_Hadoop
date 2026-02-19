# Dijkstra's Shortest Path - MapReduce Implementation

A distributed implementation of Dijkstra's shortest path algorithm using Hadoop MapReduce with automatic convergence detection.

## Features

- Iterative MapReduce approach for distributed shortest path computation
- Automatic convergence detection to minimize cluster resource usage
- Configurable source node and maximum iterations
- Handles large-scale graphs across Hadoop clusters

## Overview

This implementation finds the shortest paths from a source node (default: node 0) to all other nodes in a weighted directed graph using an iterative MapReduce approach.

## Architecture

The algorithm runs in two phases:

### Phase 1: Preprocessing (Map-only)
- Reads raw edge list from HDFS
- Initializes source node distance to 0, all others to INF
- Outputs enriched format: `src dst weight distance`

### Phase 2: Iterative Relaxation
- Each iteration relaxes distances one hop further
- Mapper: Preserves edges and emits candidate distances
- Reducer: Selects minimum distance per node
- Stops early when convergence is detected (no distance changes)

## Project Structure

```
dijkstra/
├── pom.xml                      # Maven build configuration
├── README.md                    # This file
├── Driver.java                  # Main orchestrator
├── PreprocessingMapper.java     # Graph initialization
├── MainMapper.java              # Edge relaxation
└── MainReducer.java             # Distance selection & convergence tracking
```

## Requirements

- Java 8 or higher
- Apache Hadoop 3.3.6 (or compatible version)
- Maven 3.6+ (for building)

## Input Format

Plain text file with one edge per line:
```
src dst weight
```

Example (`graph.txt`):
```
0 1 4
0 2 1
1 3 1
2 1 2
2 3 5
3 4 3
```

## Quick Start

### 1. Build

Using Maven (recommended):
```bash
cd app/dijkstra
mvn clean package
```

This creates `target/dijkstra.jar` with all dependencies configured.

Manual compilation (if Maven unavailable):
```bash
mkdir -p build
javac -cp $(hadoop classpath) -d build *.java
jar -cvf dijkstra.jar -C build .
```

### 2. Prepare Input
```bash
hadoop fs -mkdir -p /user/$(whoami)/dijkstra/input
hadoop fs -put graph.txt /user/$(whoami)/dijkstra/input/
```

### 3. Run
```bash
hadoop jar target/dijkstra.jar Driver \
  /user/$(whoami)/dijkstra/input \
  /user/$(whoami)/dijkstra/output \
  10
```

Arguments:
- Input path (HDFS)
- Output base path (HDFS)
- Max iterations (optional, default=10)

### 4. View Results
```bash
# View final results
hadoop fs -cat /user/$(whoami)/dijkstra/output/iter*/part-*

# Or download to local
hadoop fs -get /user/$(whoami)/dijkstra/output/iter* ./results/
```

## Output Format

Each line represents an edge with the computed shortest distance:
```
src dst weight distance
```

Example output:
```
0 1 4 0      # Node 0 to node 1, weight 4, distance from source: 0
0 2 1 0      # Node 0 to node 2, weight 1, distance from source: 0
1 3 1 4      # Node 1 to node 3, weight 1, distance from source: 4
```

The final iteration directory contains the shortest distances from the source node to all reachable nodes.

## Configuration

### Change Source Node
Edit `PreprocessingMapper.java`:
```java
private static final String SOURCE_NODE = "0";  // Change to desired source
```

### Adjust Hadoop Version
Edit `pom.xml`:
```xml
<hadoop.version>3.3.6</hadoop.version>  <!-- Update version here -->
```

## How It Works

### Convergence Detection
The algorithm uses Hadoop counters to track distance updates. When an iteration produces zero distance changes, convergence is achieved and execution stops early, saving cluster resources.

### Algorithm Flow
1. **Preprocessing**: Initialize source node distance to 0, others to INF
2. **Iteration 1-N**: 
   - Mapper emits candidate distances for neighbors
   - Reducer selects minimum distance per node
   - Counter tracks if any distance changed
3. **Convergence**: Stop when counter = 0 or max iterations reached

## Performance

- Time complexity: O(V × E) worst case, where V = vertices, E = edges
- Converges early for most real-world graphs
- Scales horizontally across Hadoop cluster nodes

## License

This project is part of a Hadoop learning environment.
