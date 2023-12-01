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

def job(env,JobID,SubmitTime,RunTime,Priority,Partition,Timelimit,InterArrival,NCPUS):
    queued = env.now
    #print("submit_time "+str(queued)+ "  SubmitTime "+str(SubmitTime))
    if Timelimit > NANO_TIME_LIMIT and RunTime > 0 and not Partition.endswith("384gb"):
        with node_defq.request(priority=MAX_SLURM_PRIO-Priority) as req, node_nano.request(priority=MAX_SLURM_PRIO-Priority) as sreq:
            submit_time_defq.append(queued)
            yield simpy.AnyOf(env,[req, sreq])
            wait_time_defq.append(env.now - queued)
            yield env.timeout(RunTime)
            run_time_defq.append(RunTime)
    if Timelimit > NANO_TIME_LIMIT and RunTime > 0 and Partition.endswith("384gb") :
        with node_384g.request(priority=MAX_SLURM_PRIO-Priority) as req, node_nano.request(priority=MAX_SLURM_PRIO-Priority) as sreq:
            submit_time_384g.append(queued)
            yield simpy.AnyOf(env,[req, sreq])
            wait_time_384g.append(env.now - queued)
            yield env.timeout(RunTime)
            run_time_384g.append(RunTime)

#    run_time.append(RunTime)

def run_jobs(env):
    for i in range(NUM_OF_JOBS):

       # print(df['InterArrival'][i])
## This line simulates the time since the last job was submitted...the interval between submissions.
## It is from this line that we encorporate the submission history/pattern from actual job data.
        yield env.timeout(df['InterArrival'][i]) 

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
                df['NCPUS']))

def LoadDataFrame(input_file):
    df = pd.read_csv(input_file)
    print("#Reading from ",input_file)
    return df

def shuffle_jobs(dataframe,fraction):
	df = dataframe.sample(frac = fraction,replace=True)
	df = df.sort_values(by=['SubmitTime'])
	return df
	
def usage():
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
	elif opt in ['-n']:
		NUM_OF_NANO_NODES = int(arg)
	elif opt in ['-p']:
		FRACTION=float(arg)
	elif opt in ['-h']:
		usage()

df_init  = LoadDataFrame(input_file=INPUT_FILE)
### There used to be code here in the MPI version
df = df_init

NUM_OF_JOBS = len(df)
if NUM_OF_JOBS == 0:
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


submit_time_defq= []
submit_time_384g = []
wait_time_defq  = []
wait_time_384g  = []
run_time_defq = []
run_time_384g = []

# Maximum SLURM Priority
# Priority increases with increasing size in SLURM
# but priority increases with decreasing size in Simpy
# so there must be a linear transformation.
MAX_SLURM_PRIO = 4294967295

env = simpy.Environment()
node_defq = simpy.PriorityResource(env, capacity = NUM_OF_DEFQ_NODES)
node_nano = simpy.PriorityResource(env, capacity = NUM_OF_NANO_NODES)
node_384g = simpy.PriorityResource(env, capacity = NUM_OF_384_NODES)


# Begin the simulation
env.process(run_jobs(env))
env.run()

stats_defq=np.percentile(wait_time_defq, [25, 50, 75, 95])
stats_384g=np.percentile(wait_time_384g, [25, 50, 75, 95])
run_sum_defq = np.sum(run_time_defq)
run_sum_384g = np.sum(run_time_384g)
run_mean_defq = np.average(run_time_defq)
run_mean_384g = np.average(run_time_384g)
run_med_defq = np.median(run_time_defq)
run_med_384g = np.median(run_time_384g)
jobdata_defq=len(wait_time_defq)
jobdata_384g=len(wait_time_384g)

## We're done. Print out summary.
print("#Part\t25th\t50th\t75th\t95th\tNumJobs\tagg_RT\tagg_RT/node\tnode-count")
print("defq\t%.2f\t%.2f\t%.2f\t%.2f\t%d\t%.0f\t%.0f\t%d" % (
	stats_defq[0]/3600,stats_defq[1]/3600,stats_defq[2]/3600,stats_defq[3]/3600,
	jobdata_defq,run_sum_defq/3600,run_sum_defq/3600/NUM_OF_DEFQ_NODES,NUM_OF_DEFQ_NODES))
print("384gb\t%.2f\t%.2f\t%.2f\t%.2f\t%d\t%.0f\t%.0f\t%d" % (
	stats_384g[0]/3600,stats_384g[1]/3600,stats_384g[2]/3600,stats_384g[3]/3600,
	jobdata_384g,run_sum_384g/3600,run_sum_384g/3600/NUM_OF_384_NODES,NUM_OF_384_NODES))


