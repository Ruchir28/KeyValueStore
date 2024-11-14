# Distributed Key-Value Store

A high-performance, append-only key-value store implementation with support for concurrent operations and automatic compaction.

## Overview

This project implements a distributed key-value store that uses an append-only storage mechanism with background compaction for optimal write performance and space efficiency. The system is designed to handle high-throughput concurrent operations while maintaining data consistency.

## Architecture

### Storage Mechanism

The key-value store uses an append-only storage strategy with the following characteristics:

- **Append-Only Writes**: All write operations are appended to the end of data files, ensuring fast write performance (O(1))
- **In-Memory Index**: Maintains an in-memory index mapping keys to their latest location in data files
- **Multiple Data Files**: Data is stored across multiple files with a size threshold to manage storage efficiently
- **Thread-Safe Operations**: Supports concurrent reads and writes using thread-safe data structures

## Compaction and Merge Process

The system implements an automatic compaction and merge strategy to optimize storage space and maintain performance:

### Compaction Process

1. **Trigger Conditions**
   - Activated when multiple data files exist
   - Based on configurable size thresholds
   - Runs as a background process

2. **Merge Operation**
   - Combines multiple data files into a single compact file
   - Removes obsolete key-value pairs
   - Preserves only the latest version of each key
   - Updates index locations after successful merge
   - Handles concurrent reads during merge process

3. **Space Reclamation**
   - Identifies and removes deprecated data files
   - Maintains data consistency during compaction
   - Zero-downtime compaction process
   - Atomic file operations for reliability
