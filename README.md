# Big Data Processing Lab Projects
This repository contains four lab projects (LAB1-LAB4) for the Comprehensive Big Data Processing course, implemented using Hadoop MapReduce framework.

## 🛠️ Tech Stack
- **Framework:** Hadoop MapReduce
- **Language:** Java
- **Environment​​:**
  - OS: Ubuntu 22.04.4 (VMWare)
  - Java: JDK 1.7.0
  - Hadoop: 3.2.1 (pseudo-distributed)

## LAB 1: Hadoop Standalone Setup & WordCount Experiment
Successfully set up a Hadoop standalone environment on Ubuntu VM and ran WordCount program to perform word frequency statistics on Wikipedia text data.

## LAB 2: Hadoop Inverted Index & Text Analysis
Implemented an inverted index system using Hadoop MapReduce with extended sorting and TF-IDF features for Shakespeare text analysis.

### 🔧 Features

1. Core: Inverted Index
- Mapper: Tokenizes documents, outputs <word, filename>
- Reducer: Calculates word frequency per document
- Output: word avg_freq,doc1:count;doc2:count;

2. Sorting
- Sorts inverted index results by frequency
- Implements numerical key sorting

3. Optional: TF-IDF
- Computes Term Frequency-Inverse Document Frequency
- Formula: IDF = log10(total_docs / (docs_with_term + 1))
- Output: <document, term, TF-IDF value>

Notes
- All Java tasks are executed as Hadoop MapReduce jobs, packaged as JARs.

## LAB 3: Group By Aggregation with Hadoop MapReduce & Hive Integration
Implemented SQL-like Group By operations using Hadoop MapReduce, followed by Hive table integration for query management on distributed datasets stored in HDFS.

### Task 1: Customer Data Aggregation
- Implemented MapReduce job to calculate the total account balance grouped by customer nation key.
- Equivalent SQL:
```sql
SELECT c_nationkey, SUM(c_acctbal)
FROM customer
GROUP BY c_nationkey;
```
- Output: <c_nationkey, sum(c_acctbal)>

### Task 2: Order Priority Analysis
- Implemented MapReduce job to find orders with the highest shipping priority within each order priority group.
- Equivalent SQL:
```sql
SELECT orderkey, orderpriority, MAX(shippriority)
FROM orders
GROUP BY orderpriority;
```
- Output: <order_key, order_priority, max(ship_priority)>

### 💾 Data & Hive Integration

**Input Datasets:**
- `/data/lab3/task1/customer.tbl`
- `/data/lab3/task2/orders.tbl`

**Output:**
Stored in HDFS under `/user/hc01/output/`

**Integrated results into Hive tables using:**
```sql
CREATE TABLE task1_res (
  c_nationkey STRING, 
  c_acctbal_sum STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ' ' 
LOCATION '/user/hc01/output/';
```

## LAB 4: K-Means Clustering with Hadoop MapReduce
Implemented the **K-Means clustering algorithm** using the **Hadoop MapReduce** framework to process large-scale vector datasets stored in **HDFS**.  
Each MapReduce iteration recalculates new centroids until convergence, outputting 20 final cluster centers.

### 🔧 Implementation
Each iteration runs as a **MapReduce job**, controlled by a driver program until centroids stabilize.  
- **Mapper:**  
  - Reads data points  
  - Assigns each point to the nearest centroid based on Euclidean distance  
- **Reducer:**  
  - Aggregates all points per cluster  
  - Recomputes new centroids
