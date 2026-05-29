# Parallel Processor Architecture

## Overview

Implement a parallel data processing pipeline using subagent-driven development pattern. The system coordinates multiple processor nodes that work in parallel on Kafka events.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PipelineCoordinator                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ ParserNode  │  │ CleanerNode │  │ ChunkerNode │         │
│  │  (worker)   │  │  (worker)   │  │  (worker)   │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
│  ┌─────────────┐  ┌─────────────┐                           │
│  │DedupNode    │  │VectorizerNode│                          │
│  │  (worker)   │  │  (worker)   │                           │
│  └─────────────┘  └─────────────┘                           │
└─────────────────────────────────────────────────────────────┘
         ▲                ▲                ▲
         │                │                │
    ┌────┴────┐      ┌────┴────┐      ┌────┴────┐
    │ TaskQueue │      │ TaskQueue │      │ TaskQueue │
    └─────────┘      └─────────┘      └─────────┘
```

## Components

### 1. PipelineCoordinator

Central orchestrator that:
- Manages worker pool lifecycle
- Routes events through processing stages
- Monitors health and load balancing
- Handles graceful shutdown

**File:** `src/processors/parallel/coordinator.py`

### 2. ProcessorNode

Base class for processor workers:
- Async message handling
- Health status reporting
- Error recovery with backoff
- Metrics collection

**File:** `src/processors/parallel/node.py`

### 3. WorkerPool

Manages concurrent execution:
- Configurable concurrency limit
- Worker lifecycle management
- Task distribution
- Load balancing

**File:** `src/processors/parallel/pool.py`

### 4. TaskQueue

Thread-safe task queue:
- Priority support
- Backpressure handling
- Task deduplication
- Metrics tracking

**File:** `src/processors/parallel/queue.py`

## Tasks

### Task 1: Create directory structure and base classes

Create `src/processors/parallel/` with:
- `__init__.py`
- `coordinator.py` - PipelineCoordinator class
- `node.py` - ProcessorNode base class
- `pool.py` - WorkerPool class
- `queue.py` - TaskQueue class

### Task 2: Implement ProcessorNode base class

Base class with:
- Async start/stop methods
- Health check method
- Error handling with backoff
- Abstract process() method

### Task 3: Implement WorkerPool

Worker pool with:
- Configurable max_workers
- Context manager support
- Task scheduling with round-robin
- Worker health monitoring

### Task 4: Implement TaskQueue

Thread-safe queue with:
- Priority levels (HIGH, NORMAL, LOW)
- Max size with backpressure
- Task deduplication by content hash
- Async put/get operations

### Task 5: Implement PipelineCoordinator

Main coordinator with:
- Node registration and lifecycle
- Event routing to appropriate nodes
- Health monitoring aggregation
- Graceful shutdown sequence

### Task 6: Integrate with Kafka consumer

Update `src/kafka/consumer.py`:
- Replace simple handler with PipelineCoordinator
- Route events to appropriate processor nodes
- Handle consumer group coordination

### Task 7: Add unit tests

Test coverage target: 80%+
- Test each class in isolation
- Mock external dependencies (Kafka, storage)
- Verify error handling and recovery

## File Structure

```
src/processors/parallel/
├── __init__.py
├── coordinator.py   # PipelineCoordinator
├── node.py          # ProcessorNode (base)
├── pool.py          # WorkerPool
└── queue.py         # TaskQueue
```

## Dependencies

- asyncio for concurrency
- collections for deque (TaskQueue)
- dataclasses for immutable configs
- typing for Protocol (interface definitions)

## Acceptance Criteria

1. Each processor node runs independently
2. Worker pool limits concurrent operations
3. Task queue provides backpressure
4. Coordinator routes events correctly
5. Graceful shutdown on SIGTERM
6. 80%+ test coverage