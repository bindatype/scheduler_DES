debug = False
#tiny      8:00:00 = 240 minutes
#short  1-00:00:00 = 1440
#defq* 14-00:00:00 =

import sys
import getopt
import pandas as pd
pd.options.mode.chained_assignment = None 
import simpy
import numpy as np
#import matplotlib.pyplot as plt
from mpi4py import MPI


def job(env,JobID,SubmitTime,RunTime,Priority,Partition,Timelimit,InterArrival,NCPUS,node):
    queued = env.now
    submit_time.append(queued)
    #print("submit_time "+str(queued)+ "SubmitTime "+str(SubmitTime))
    with node.request(priority=MAX_SLURM_PRIO-Priority) as req:
        yield req
        wait_time.append(env.now - queued)
        yield env.timeout(RunTime)
        run_time.append(RunTime)

def run_jobs(env):
    for i in range(NUM_OF_JOBS):

## This line simulates the time since the last job was submitted...the interval between submissions.
## It is from this line that we get the submission history/pattern from Pegasus.
        yield env.timeout(df['InterArrival'][i]) # Change to waittime at some point?

## Added loop for multinode jobs and to approximate mpi jobs.
        for j in range(int(df['NNodes'][i])):
            env.process(job(env, 
                df['JobID'][i],
                df['SubmitTime'][i],
                df['RunTime'][i],
                df['Priority'][i],
                df['Partition'][i],
                df['Timelimit'][i],
                df['InterArrival'][i],
                df['NCPUS'], 
                node=node))

def LoadDataFrame(input_file):
    df = pd.read_csv(input_file)
    if rank == 0:
       print("#Reading from ",input_file)
    return df

def shuffle_jobs(dataframe,fraction):
	df = dataframe.sample(frac = fraction,replace=True)
	df = df.sort_values(by=['SubmitTime'])
	return df
	
def usage():
    if rank == 0:
        print("All arguments are required")
        print("\t-f Input file name")
        print("\t-d Number of defq nodes")
        print("\t-t Number of 384 nodes")
        print("\t-n Number of nano nodes")
        print("\t-p Fraction resampled. Negative number to skip.")
        print("Example: mpiexec -n 4 python3 scheduler.py -f $NAME -d 54 -n 30 -p -1 ")
    exit(1)

try:
	opts, args = getopt.getopt(sys.argv[1:], "f:d:t:n:p:")
except getopt.GetoptError:
    usage()



comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


NANO_TIME_LIMIT = 30
_384_TIME_LIMIT = 240
SHORT_TIME_LIMIT = 1440
DEFQ_TIME_LIMIT = 20160

# Define default parameter values
NUM_OF_DEFQ_NODES=0
NUM_OF_NANO_NODES=0
NUM_OF_384_NODES = 0
#NUM_OF_DEFQ_NODES = None
#NUM_OF_384_NODES = None
#NUM_OF_NANO_NODES = None
INPUT_FILE = None
FRACTION = -1

for opt, arg in opts:
	if opt in ['-f']:
		INPUT_FILE = arg
	elif opt in ['-d']:
		NUM_OF_DEFQ_NODES = int(arg)
	elif opt in ['-t']:
		NUM_OF_384_NODES = int(arg)
	elif opt in ['-p']:
		FRACTION=float(arg)
	elif opt in ['-n']:
		NUM_OF_NANO_NODES = int(arg)
	elif opt in ['-h']:
		usage()

TOTAL_NUM_OF_NODES = NUM_OF_DEFQ_NODES+NUM_OF_NANO_NODES+NUM_OF_384_NODES

#Maximum SLURM Priority
# Priority increases with increasing size in SLURM
# but priority increases with decreasing size in Simpy
# so there must be a linear transformation.
MAX_SLURM_PRIO = 4294967295

df_init  = LoadDataFrame(input_file=INPUT_FILE)

# Each worker should project out their own dataframe from the master dataframe.
if rank == 0:
	df = df_init[((df_init.Timelimit > NANO_TIME_LIMIT)) & (df_init.RunTime >= 5)]
if rank == 1:
	df = df_init[((df_init.Timelimit <= NANO_TIME_LIMIT))  & (df_init.RunTime >= 5)]
if rank == 2:
	df = df_init[(df_init.Partition.str.endswith("384gb"))]

if len(df) == 0:
	print("One or more processes failed because it's dataframe had zero length.")
	comm.Abort()

## Set resampling fraction. Negative value means skip. 
if FRACTION >=0:
	df=shuffle_jobs(dataframe=df,fraction=FRACTION)

## Reset indices to 0,1,2... drop true so that new indices do not get stored as a new column. 
## inplace True operate on dataframe ... don't assign to new variable.
df.reset_index(drop=True,inplace=True)

## Subtract off offset so first job gets submitted at time = 0. 
## df[].diff diffs submittimes to get intervals in between which 
## will be used as env.timeouts() later on.
df['SubmitTime']=df['SubmitTime']-df['SubmitTime'].min()
df['InterArrival']=df['SubmitTime'].diff(periods=1)
df["InterArrival"][0]=df["SubmitTime"][0]

if debug:
	with pd.option_context('display.max_rows', None,
                       'display.max_columns', None,'display.width',None,
                       'display.precision', 3,
                       ):
		print(df)
if debug:
    print(df[df['InterArrival'].isnull()]) 

NUM_OF_JOBS = len(df)

submit_time = []
wait_time  = []
run_time = []

env = simpy.Environment()
if rank == 0:
	node = simpy.PriorityResource(env, capacity = NUM_OF_DEFQ_NODES)
if rank == 1:
	node = simpy.PriorityResource(env, capacity = NUM_OF_NANO_NODES)
if rank == 2:
	node = simpy.PriorityResource(env, capacity = NUM_OF_384_NODES)

env.process(run_jobs(env))
env.run()

stats=np.percentile(wait_time, [25, 50, 75, 95])

run_sum = np.sum(run_time)
run_mean = np.average(run_time)
run_med = np.median(run_time)

## Wait until all processes are done before aggregating statistics.
comm.Barrier()
stats=comm.gather(stats, root=0)
jobdata=comm.gather(len(df),root=0)
run_sum=comm.gather(run_sum,root=0)
total_jobs = comm.reduce(len(df), op=MPI.SUM,root=0)


## We're done. Print out summary.
if rank == 0:
	#print("#Total Jobs %d" %(total_jobs))
	print("#Part\t25th\t50th\t75th\t95th\tNumJobs\tagg_RT\tagg_RT/node\tnode-count")
	print("defq\t%.2f\t%.2f\t%.2f\t%.2f\t%d\t%.0f\t%.0f\t%d" % (
		stats[0][0]/3600,stats[0][1]/3600,stats[0][2]/3600,stats[0][3]/3600,
		jobdata[0],run_sum[0]/3600,run_sum[0]/3600/NUM_OF_DEFQ_NODES,NUM_OF_DEFQ_NODES))
	if size >= 2:
		print("nano\t%.2f\t%.2f\t%.2f\t%.2f\t%d\t%.0f\t%.0f\t%d" % (
			stats[1][0]/3600,stats[1][1]/3600,stats[1][2]/3600,stats[1][3]/3600,
			jobdata[1],run_sum[1]/3600,run_sum[1]/3600/NUM_OF_NANO_NODES,NUM_OF_NANO_NODES))
	if size >= 3:
		print("384gb\t%.2f\t%.2f\t%.2f\t%.2f\t%d\t%.0f\t%.0f\t%d" % (
			stats[2][0]/3600,stats[2][1]/3600,stats[2][2]/3600,stats[2][3]/3600,
			jobdata[2],run_sum[2]/3600,run_sum[2]/3600/NUM_OF_384_NODES,NUM_OF_384_NODES))


