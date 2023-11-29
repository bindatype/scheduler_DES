# scheduler_DES
A project to replay a High Performance Computer's job history but allowing for varying the number of partitions or nodes within a partition. 
The result can be used to make informed estimates for how changes to a cluster's inventory of nodes may impact the amount of time jobs spend
waiting to dequeue. 
 
For example, one might ask how adding 5 more GPU nodes might relieve job pressure, what is the effect interactive allocations are having on 
job throughput, or if timelimits are reasonable, all based on actual historical job data collected from SLURM's sacct command.  

## Getting Started: Setting up a conda env

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
