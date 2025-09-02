# Zig Database Studies

A repository dedicated to **practical database studies**, with all implementations written in [Zig](https://ziglang.org/).  
The goal of this project is to incrementally build fundamental database components, learning how real-world databases work under the hood.

## 📂 Project Structure

This repository contains three main folders, each representing a stage of increasing complexity:

1. [🔑 Simple Key-Value Store](https://github.com/josethz00/zig-database-studies/tree/main/simple-kv)  
   A minimal in-memory key-value store with write-ahead logging (WAL) and basic persistence.

2. [🌲 LSM-Tree](https://github.com/josethz00/zig-database-studies/tree/main/lsm-tree)  
   An implementation of Log-Structured Merge Trees, exploring compaction, SSTables, and write optimization.

3. [📖 B+Tree](https://github.com/josethz00/zig-database-studies/tree/main/b-tree)  
   A B+Tree index structure, commonly used in relational databases for efficient range queries.

## 🚀 Goals

- Learn core database internals through hands-on implementations.  
- Explore storage engines and indexing strategies.  
- Improve proficiency in Zig while tackling systems programming challenges.  

## 🛠️ Requirements

- [Zig](https://ziglang.org/download/) (0.15.1 version)  

## 📌 Notes

This repository is for **learning and experimentation purposes**.  
The implementations prioritize clarity and educational value over production readiness.
