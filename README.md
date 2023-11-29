# scheduler_DES
A project to replay a High Performance Computer's job history while allowing for varying the number of partitions or nodes within a partition. 
The result can be used to make informed estimates for how changes to a cluster's inventory of nodes may impact the amount of time jobs spend
waiting to dequeue. 


## Introduction
Suppose you have a 40-node or 500-node SLURM cluster (the actual number of nodes doesn't matter) that you have been operating for some 
amount of time, say one year, and you want to investigate how adding 10 or 20 more nodes might relieve job pressure and shorten wait times. 
Additionally, one might inquire as to what is the effect interactive allocations are having on job throughput or if timelimits are 
reasonable; all based on actual historical job data collected from SLURM's sacct command. 

## Getting Started: Setting up a conda env
To get started using scheduler_DES make sure you have set up and environment with the following packages:
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
In the above example, we requested that jobs summarized in example_data.txt were processed with 125 defq nodes, 15 nano nodes, and 24 384gb
nodes. And the resampling parameter, p, was set to 1.  

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
The output includes the 25th, 50th, 75th, and 95th percentiles for time spent waiting to dequeue as well as aggregate runtime and aggregate
runtime per node. 

## Limitations
* Resampling doesn't quite work the way one would expect and that is because the actual historical submit times are used by the simulation as 
submit times for the virtual jobs. Randomly removing say, half the jobs, lightens the duty factor of the simuation nodes by half and 
percentiles look very small. This is because the dequeue times for those jobs that are sampled are still those from the historical record even
though those jobs are being resampled as a member of a smaller population. Instead of 
```
||||||||| -> ||||| Duty cycle stays the same
ti      tf   ti  tf
```
-we get half the jobs over the same time period. The simulated cluster will have plenty of downtime to dequeue jobs. 
```
||||||||| -> || |  || | Duty cycle decreases 
ti      tf   ti       tf
```

* For two overlapping partitions, A and B, the intersection of those partitions A ∩ B is not handled yet. By the time the dataframes are created
in each process, it has already been decided if the job will be processed in A or B so there's really no place for the intersection of 
partitions as the code is written. There's probably some clever work-around to the issue but nothing that exists presently.

* The partitions in the command line options are not flexible. If you want to use the 384gb option then you must use the nano option. Nano is
mapped to rank 1 and 384gb is mapped to rank 2.   

* Doesn't handle true MPI-style jobs where a job requiring N nodes will wait until all N nodes are free before dequeueing while this simulation
simply addes N jobs, each with identical parameters (such as submit time) to the event generator.   
