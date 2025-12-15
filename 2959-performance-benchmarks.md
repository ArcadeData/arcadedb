# Issue #2959: HA - Task 5.2 - Add Performance Benchmarks

## Overview
Add JMH (Java Microbenchmark Harness) benchmarks to measure HA cluster performance characteristics.

## Objectives
1. Create ReplicationThroughputBenchmark - Measure tx/sec with various quorum settings
2. Create FailoverTimeBenchmark - Measure time from leader death to new leader election
3. Create ElectionTimeBenchmark - Measure leader election time

## Scope
- Create benchmark classes using JMH framework
- Measure replication throughput with different quorum configurations
- Measure failover time for HA resilience
- Measure leader election performance

## Progress Log

### Step 1: Branch Verification ✓
**Started**: 2025-12-15
- Current branch: `feature/2043-ha-test` (continuing on existing HA feature branch)

### Step 2: Documentation Created ✓
**Completed**: 2025-12-15
- Created tracking document: `2959-performance-benchmarks.md`

### Step 3: Analysis Phase
**Started**: 2025-12-15

#### Requirements from Issue:
- Add JMH benchmarks for HA performance
- Measure replication throughput with various quorum settings
- Measure failover time from leader death to new leader
- Low effort, P4 priority task

#### Files to Create:
1. `ReplicationThroughputBenchmark.java` - Measure tx/sec with quorum settings
2. `FailoverTimeBenchmark.java` - Measure leader failover time
3. `ElectionTimeBenchmark.java` - Measure leader election time

### Step 4: Implementation Complete ✓
**Completed**: 2025-12-15

#### Infrastructure Decision:
- Project uses custom benchmark approach (not JMH)
- Benchmarks extend ContainersTestTemplate
- Created in resilience module under new `com.arcadedb.containers.performance` package

#### Benchmarks Created:

**1. ReplicationThroughputBenchmark.java** (152 lines)
- Measures transaction throughput (tx/sec) with various quorum settings
- Tests: MAJORITY, ALL, NONE quorum configurations
- Features:
  - Warmup phase (100 transactions)
  - Benchmark phase (1000 transactions)
  - Metrics: throughput, average latency, replication verification
  - 3-node cluster with majority quorum

**2. FailoverTimeBenchmark.java** (266 lines)
- Measures leader failover time from death to recovery
- Tests: Normal failover, failover under load
- Features:
  - Warmup iterations (3)
  - Benchmark iterations (10)
  - Statistics: average, min, max, range
  - Automatic cluster recovery verification
  - Background load testing capability

**3. ElectionTimeBenchmark.java** (244 lines)
- Measures leader election time for new clusters
- Tests: 3-node election, 2-node election, re-election after failure
- Features:
  - Single-iteration measurements (run test multiple times for statistics)
  - Various cluster configurations
  - Re-election after leader failure scenario

#### Implementation Notes:
- All benchmarks use Testcontainers for cluster management
- Toxiproxy integration for network fault injection
- Awaitility for async operation verification
- DatabaseWrapper for remote database operations
- Comprehensive logging with structured output
- Ready for CI/CD integration

#### Running Benchmarks:
```bash
# Run all performance benchmarks
mvn test -pl resilience -Dtest="*Benchmark"

# Run specific benchmark
mvn test -pl resilience -Dtest=ReplicationThroughputBenchmark
mvn test -pl resilience -Dtest=FailoverTimeBenchmark
mvn test -pl resilience -Dtest=ElectionTimeBenchmark
```

## Technical Notes
- Part of Phase 5: Improve Test Infrastructure from HA_IMPROVEMENT_PLAN.md
- Related to issue #2043
- Uses custom benchmark framework (not JMH)
- Provides meaningful performance metrics for HA operations
- Benchmarks measure real-world HA scenarios with Testcontainers
