# scheduler_DES
A project to replay a High Performance Computer's job history but allowing to alter the number of partitions or nodes within a partition. 

## Set up a conda env:

```
conda create --name scheduler_DES -y
conda activate scheduler_DES
conda install -c conda-forge -y simpy
conda install -c conda-forge -y mpi4py
conda install -c conda-forge -y pandas
conda update -n base -c defaults conda
```


```
Usage: 
All arguments are required
	-f Input file name
	-d Number of defq nodes
	-t Number of 384 nodes
	-n Number of nano nodes
	-p Fraction resampled. Negative number to skip.
Example: NAME=example_data.txt; mpiexec -n 3 python3 scheduler.py -f $NAME -d 125 -n 15 -t 24 -p 1
	There should be as many mpi processes as there are partitions.  
```
## Invoking the Simulation and Output
Invocation and output will appear similar to 
```
$ NAME=example_data.txt; mpiexec -n 3 python3 scheduler.py -f $NAME -d 125 -n 15  -t 24
#Reading from  example_data.txt
#Part	25th	50th	75th	95th	NumJobs	agg_RT	agg_RT/node	node-count
defq	0.80	5.43	48.25	326.79	49079	545272	4362	125
nano	0.73	3.41	8.65	27.84	80920	7264	484	15
384gb	0.00	13.17	75.92	176.33	2047	50334	2097	24
```
