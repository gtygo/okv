# okv
一个可靠，轻量，高性能的嵌入式KV数据库，使用Go语言实现。

## 特性
- 基于BitCask存储模型构建，读写效率非常高
- 支持事物，提供写事务提交时崩溃恢复机制

## 计划
- ~~v1.0 实现BitCask模型存储，实现基本get put delete操作~~
- v2.0 优化性能，减轻gc压力，支持更多数据结构