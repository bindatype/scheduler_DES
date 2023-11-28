Set up a conda env:
conda create --name scheduler_DES -y
conda activate scheduler_DES
conda install -c conda-forge -y simpy
conda install -c conda-forge -y mpi4py
conda install -c conda-forge -y pandas
conda update -n base -c defaults conda




Usage: 
All arguments are required
	-f Input file name
	-d Number of defq nodes
	-t Number of 384 nodes
	-n Number of nano nodes
	-p Fraction resampled. Negative number to skip.
Example: NAME=example_data.txt; time mpiexec -n 3 python3 dev2.py -f $NAME -d 125 -n 15  -t 24 -p 1
	There should be as many mpi processes as there are partitions.  

