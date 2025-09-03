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
