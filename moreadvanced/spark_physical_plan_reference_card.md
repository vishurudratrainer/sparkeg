# 📖 PySpark Masterclass: Physical Plan Reference Card

> **Quick Tip:** Call `.explain(True)` on any DataFrame to inspect its logical and physical execution plans.

---

## 🔍 Key Physical Plan Operators & Indicators

| Plan Keyword | What It Means | Impact | Action / Recommendation |
| :--- | :--- | :---: | :--- |
| **`PushedFilters`** | Storage layer skips non-matching rows before loading memory. | 🟢 **GREAT** | Ensure early `.filter()` calls when reading Parquet/Delta files. |
| **`Batched: true`** | Spark processes data in vectorized columnar batches. | 🟢 **GREAT** | Default for Parquet/ORC; avoids slow object-by-object reads. |
| **`WholeStageCodegen` (`*(1)`)** | Operators compiled into a single Java bytecode execution loop. | 🟢 **GREAT** | Maximizes CPU cache efficiency; preserve native expressions. |
| **`BroadcastHashJoin`** | Driver copies small lookup table directly to all workers. | 🟢 **GREAT** | **Best for joins.** Eliminates network shuffle overhead entirely. |
| **`CustomShuffleReader (coalesced)`** | AQE dynamically merges tiny shuffle partitions into ~128MB chunks. | 🟢 **GREAT** | Confirms AQE is fixing small partition/file issues automatically. |
| **`CustomShuffleReader (skewed)`** | AQE splits a bottlenecked partition key into parallel sub-tasks. | 🟢 **GREAT** | Confirms AQE is mitigating data skew bottlenecks at runtime. |
| **`ColumnarToRow`** | Converts vectorized data batches into standard row structures. | 🟡 **NEUTRAL** | Normal transition; verify it isn't triggered prematurely by UDFs. |
| **`SortMergeJoin`** | Both sides of a dataset are shuffled across the network and sorted. | 🔴 **EXPENSIVE** | Default for large tables. Prefer broadcast if one table is small. |
| **`Exchange hashpartitioning`** | Data is being shuffled across the cluster network. | 🔴 **EXPENSIVE** | High network I/O. Check if shuffles can be reduced or bucketed. |

---

## ⚡ The 3-Second "Squint Test" Checklist

Before executing heavy pipelines in production, scan your physical plan for these three visual landmarks:

1. **Check for `Exchange` operators:** Are there unnecessary network shuffles? Can any be replaced with a `BroadcastHashJoin`?
2. **Inspect the `FileScan` line:** Are `PushedFilters` present? If missing on Parquet or Delta reads, verify your filter placement.
3. **Verify AQE status:** Look for `AdaptiveSparkPlan isFinalPlan=false` (pre-execution setup) or `isFinalPlan=true` (post-execution runtime optimized).

---

## 🛠️ Essential Configuration Keys

```python
# Enable Adaptive Query Execution (AQE)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Control Auto-Broadcast Threshold (default: 10MB = 10485760 bytes)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

# Disable Auto-Broadcast to force Sort-Merge Join (useful for lab testing)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```