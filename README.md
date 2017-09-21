Trajectory Similarity Search in Apache Spark
============================================
This project implements the trajectory simiarltiy algorithm and all its competitors described in [this paper](http://www.vldb.org/pvldb/vol10/p1478-xie.pdf).

Implemented algorithms and variants include:
- **DualIndexingSolution**: Roaring Bitmap DFT w/ Dual Indexing
- **RRSolution**: Roaring Bitmap DFT w/o Dual Indexing
- **BFDISolution**: Bloom Filter DFT w/ Dual Indexing
- **BloomFilterSolution**: Bloom Filter DFT w/o Dual Indexing
- **BitMapSolution**: Raw Bitmap DFT
- **TrajIndexingSolution**: Distributed R-Tree on Bounding Boxes.
- **VPTreeSolution**: Distributed VP-Tree over Trajectories
- **MTreeSolution**: Distributed M-Tree over Trajectories.
- **BaseLine**: Brute Force Top-k

Build
-----
Call `sbt assembly` and you will get the compiled package at `target/scala-2.11/traj-sim-assembly-1.0.jar`.

Run
---
Run it by feeding the package to `spark-submit`, the entry point of different algorithms (listed above) and other utilities are located at `edu.utah.cs.trajecotry`.

Contributor
-----------
- Dong Xie: dongx [at] cs [dot] utah [dot] edu