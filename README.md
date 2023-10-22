# CompreGel: Efficient Distributed Graph Propagation via Error-Bounded Lossy Message Compression
Source code of paper “CompreGel: Efficient Distributed Graph Propagation via Error-Bounded Lossy Message Compression”. 

## Datasets
We provide a toy dataset in `data/`. 

The data format follows [Pregel+](http://www.cse.cuhk.edu.hk/pregelplus/download.html), i.e., 

`Line format:    vertex-ID    \t    number-of-neighbors    neighbor1-ID    neighbor2-ID    •••`

The COO format dataset can be converted to the above format with `data/format.py`, 

```
    python format.py <input file>
```
All the datasets can be found on SNAP and LAW. 

## Experiment
### Tested Environment
* RedHatEnterpriseServer 7.9
* C++ 14
* g++ - 7.2.0

### Dependency
[SZ compressor](https://github.com/szcompressor/SZ3/tree/master) is required. 
* mkdir build && cd build
* cmake -DCMAKE_INSTALL_PREFIX:PATH=[INSTALL_DIR] ..
* make
* make install
Then, you'll find all the executables in [INSTALL_DIR]/bin and header files in [INSTALL_DIR]/include

We suggest cloning https://github.com/szcompressor/SZ3.git to the folder `../data` and setting [INSTALL_DIR] to the folder `..`. 

### Usage Instruction
Folder `PageRank` is for PageRank computation, folder `PPR` is for Personalized PageRank (PPR) computation, and folder `HKPR` is for Heat Kernel PageRank (HKPR) computation. 

The instructions for the three algorithms are quite similar. 

First, load data
```
cd data
hadoop fs -put toy.txt </toyFolder>
```
`toyFolder` is user-specified. It requires to be consistent with the first parameter in function `pregel_pagerank` (or `pregel_ppr`, or `pregel_hkpr`) in `PageRank/run.cpp` (or `PPR/run.cpp`, or `HKPR/run.cpp`). 

Then, run the script. We include the fundamental commands in the script file: `PageRank/run.sh` (also in `PPR/run.sh` and `HKPR/run.sh`)

```
cd PageRank (or `cd PPR`, or `cd HKPR`)
./run.sh
```

Alternatively, it can be executed mannually

```
cd PageRank (or `cd PPR`, or `cd HKPR`)
rm run
make run
```
For PageRank,
```
LD_PRELOAD=../SZ3/lib64/libzstd.so mpiexec -n 32 -f ./conf ./run <num of iteration> <SZ error bound>
```
For PPR,
```
LD_PRELOAD=../SZ3/lib64/libzstd.so mpiexec -n 32 -f ./conf ./run <num of iteration> <source vertex ID> <SZ error bound>
```
For HKPR,
```
LD_PRELOAD=../SZ3/lib64/libzstd.so mpiexec -n 32 -f ./conf ./run <num of iteration> <source vertex ID> <parameter t> <SZ error bound>
```
`<SZ error bound>` is optional and the default value is set to 1E-10. 

### Results

#### PageRank

| Dataset | $n$ | $m$ | time/sec | speedup | 
|-------------|:------:|:---------:|:-:|:--:|
| LiveJournal | 4,847,571 | 68,993,773 | 5.011 | 2.4x |
| Friendster | 65,608,366 | 1,806,067,135 | 78.491 | 2.9x |

#### Personalized PageRank (PPR)

| Dataset | $n$ | $m$ | time/sec | speedup | 
|-------------|:------:|:---------:|:-:|:--:|
| UK-2005 | 39,454,463 | 783,027,125 | 10.326 | 12.0x |
| Webbase |  115,554,441 | 854,809,761 | 12.547 | 7.9x |

#### Heat Kernel PageRank (HKPR)

| Dataset | $n$ | $m$ | time/sec | speedup | 
|-------------|:------:|:---------:|:-:|:--:|
| UK-2005 | 39,454,463 | 783,027,125 | 7.441 | 10.9x |
| Webbase |  115,554,441 | 854,809,761 | 13.851 | 6.4x |

