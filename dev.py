debug = False
#tiny      8:00:00 = 240 minutes
#short  1-00:00:00 = 1440
#defq* 14-00:00:00 =


NANO_TIME_LIMIT = 30
TINY_TIME_LIMIT = 240
SHORT_TIME_LIMIT = 1440
DEFQ_TIME_LIMIT = 20160
# Total Nodes for CPU jobs = 160
AVAILABLE_NODES = 160
NUM_OF_DEFQ_NODES=32
NUM_OF_NANO_NODES=0
NUM_OF_SHORT_NODES=32
NUM_OF_TINY_NODES = 48


import sys
import getopt
import pandas as pd
pd.options.mode.chained_assignment = None 
import simpy
import numpy as np
#import matplotlib.pyplot as plt
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def shuffle_jobs(dataframe,fraction):
	df = dataframe.sample(frac = fraction,replace=True)
	df = df.sort_values(by=['SubmitTime'])
	return df
	
def usage():
    if rank == 0:
        print("All arguments are required")
        print("\t-f Input file name")
        print("\t-d Number of defq nodes")
#        print("\t-s Number of short nodes")
#        print("\t-t Number of tiny nodes")
        print("\t-n Number of nano nodes")
        print("\t-p Fraction resampled. Negative number to skip.")
        print("Example: mpiexec -n 4 python3 scheduler_DES.py -f $NAME -d 54 -n 30 -p -1 ")
#        print("\tThere should be as many mpi processes as there are partitions. In the current implementation mpiexec -n")
    exit(1)

try:
	opts, args = getopt.getopt(sys.argv[1:], "f:d:s:t:n:p:")
except getopt.GetoptError:
    usage()

MODE = None
INPUT_FILE = None
NUM_OF_DEFQ_NODES = None
NUM_OF_SHORT_NODES = None
NUM_OF_TINY_NODES = None
NUM_OF_NANO_NODES = None

for opt, arg in opts:
	if opt in ['-f']:
		INPUT_FILE = arg
	elif opt in ['-d']:
		NUM_OF_DEFQ_NODES = int(arg)
	elif opt in ['-s']:
		NUM_OF_SHORT_NODES = int(arg)
	elif opt in ['-t']:
		NUM_OF_TINY_NODES = int(arg)
	elif opt in ['-p']:
		FRACTION=float(arg)
	elif opt in ['-n']:
		NUM_OF_NANO_NODES = int(arg)
	elif opt in ['-h']:
		usage()


TOTAL_NUM_OF_NODES = NUM_OF_DEFQ_NODES+NUM_OF_NANO_NODES 
#+NUM_OF_SHORT_NODES+NUM_OF_TINY_NODES+NUM_OF_NANO_NODES
if TOTAL_NUM_OF_NODES != AVAILABLE_NODES:
	if rank == 0:
		print("Number of CPU nodes must equal 160!")
#	exit(1)

#Maximum SLURM Priority
# Priority increases with increasing size in SLURM
# but priority increases with decreasing size in Simpy
# so there must be a linear transformation.
MAX_SLURM_PRIO = 4294967295

def ReadData():
    df = pd.read_csv(INPUT_FILE)
    if rank == 0:
       print("Reading from ",INPUT_FILE)
    return df


df_init  = ReadData()

# Each worker should project out their own dataframe from the master dataframe.
if rank == 0:
	df = df_init[((df_init.Timelimit > NANO_TIME_LIMIT)) & (df_init.RunTime >= 5)]
#		df = df_init[((df_init.Timelimit > NANO_TIME_LIMIT) & ( df_init.Partition!='small-gpu') & (df_init.Partition!='large-gpu'))]
if rank == 1:
	df = df_init[((df_init.Timelimit <= NANO_TIME_LIMIT))  & (df_init.RunTime >= 5)]
#		df = df_init[((df_init.Timelimit <= NANO_TIME_LIMIT) & ( df_init.Partition!='small-gpu') & (df_init.Partition!='large-gpu'))]
if rank == 2:
	df = df_init[(df_init.Timelimit <= TINY_TIME_LIMIT) & (df_init.Timelimit > NANO_TIME_LIMIT)]
if rank == 3:
	df = df_init[df_init.Timelimit <= NANO_TIME_LIMIT]

#print(f"Rank: "+str(rank)+"\nDF Length: "+str(len(df)))

if FRACTION >=0:
	df=shuffle_jobs(dataframe=df,fraction=FRACTION)
#	dff=shuffle_jobs(dataframe=df,fraction=FRACTION)

#dff.reset_index(drop=True,inplace=True)
df.reset_index(drop=True,inplace=True)
#print("df['JobID'][99] "+str(df['JobID'][99])+"\ndff['JobID'][99] "+str(dff['JobID'][99])+"\n\tdf['SubmitTime'][99] "+str(df['SubmitTime'][99])+"\n\tdff['SubmitTime'][99] "+str(dff['SubmitTime'][99]))
#print("df['JobID'][100] "+str(df['JobID'][100])+"\ndff['JobID'][100] "+str(dff['JobID'][100])+"\n\tdf['SubmitTime'][100] "+str(df['SubmitTime'][100])+"\n\tdff['SubmitTime'][100] "+str(dff['SubmitTime'][100]))
#print("df['JobID'][101] "+str(df['JobID'][101])+"\ndff['JobID'][101] "+str(dff['JobID'][101])+"\n\tdf['SubmitTime'][101] "+str(df['SubmitTime'][101])+"\n\tdff['SubmitTime'][101] "+str(dff['SubmitTime'][101]))

#if len(df) == 0:
print(f"Rank: "+str(rank)+"\nDF Length: "+str(len(df)))
#print(f"Rank: "+str(rank)+"\nDF Length: "+str(len(df_init[((df_init.Timelimit <= NANO_TIME_LIMIT))])))
#print(f"Submit Time: "+str(df['SubmitTime'].min()))
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

def job(env,JobID,SubmitTime,RunTime,Priority,Partition,Timelimit,InterArrival,NCPUS,node):
    queued = env.now
    submit_time.append(queued)
    with node.request(priority=MAX_SLURM_PRIO-Priority) as req:
        yield req
        wait_time.append(env.now - queued)
        yield env.timeout(RunTime)
        run_time.append(RunTime)

def run_jobs(env):
    for i in range(NUM_OF_JOBS):
        #print(df['InterArrival'][i])
        yield env.timeout(df['InterArrival'][i]) # Change to waittime at some point?
        for j in range(int(df['NNodes'][i])):
            env.process(job(env, df['JobID'][i],df['SubmitTime'][i],df['RunTime'][i],df['Priority'][i],df['Partition'][i],df['Timelimit'][i],df['InterArrival'][i],df['NCPUS'], node=node))    
#        print(df['NNodes'][i])



env = simpy.Environment()
if rank == 0:
	node = simpy.PriorityResource(env, capacity = NUM_OF_DEFQ_NODES)
if rank == 1:
	node = simpy.PriorityResource(env, capacity = NUM_OF_NANO_NODES)
if rank == 2:
	node = simpy.PriorityResource(env, capacity = NUM_OF_TINY_NODES)
if rank == 3:
	node = simpy.PriorityResource(env, capacity = NUM_OF_NANO_NODES)

env.process(run_jobs(env))
env.run()
#env.run(until=1000)

stats=np.percentile(wait_time, [25, 50, 75, 95])

run_sum = np.sum(run_time)
#print(f"Rank: "+str(rank)+"\nDF Run Length: "+str(run_sum/3600))
run_mean = np.average(run_time)
run_med = np.median(run_time)


comm.Barrier()
stats=comm.gather(stats, root=0)
#print(f"Rank: "+str(rank)+"\nJobs: "+str(len(df)))
jobdata=comm.gather(len(df),root=0)
run_sum=comm.gather(run_sum,root=0)

total_jobs = comm.reduce(len(df), op=MPI.SUM,root=0)
if rank == 0:
	#print("Mode %s, Defq %d, Short %d, Tiny %d, Nano %d :: Short TL %d, Tiny TL %d, Nano TL %d" %(MODE,NUM_OF_DEFQ_NODES,NUM_OF_SHORT_NODES,NUM_OF_TINY_NODES,NUM_OF_NANO_NODES,SHORT_TIME_LIMIT,TINY_TIME_LIMIT,NANO_TIME_LIMIT))
##	print("Mode %s, Defq %d" %(MODE,NUM_OF_DEFQ_NODES))
	print("Total Jobs %d" %(total_jobs))
	print("Part\t25th\t50th\t75th\t95th\tNumJobs\tagg_RT\tagg_RT/node\tnode-count")
	print("defq\t%.2f\t%.2f\t%.2f\t%.2f\t%d\t%.0f\t%.0f\t%d" % (stats[0][0]/3600,stats[0][1]/3600,stats[0][2]/3600,stats[0][3]/3600,jobdata[0],run_sum[0]/3600,run_sum[0]/3600/NUM_OF_DEFQ_NODES,NUM_OF_DEFQ_NODES))
	if size == 2:
		print("nano\t%.2f\t%.2f\t%.2f\t%.2f\t%d\t%.0f\t%.0f\t%d" % (stats[1][0]/3600,stats[1][1]/3600,stats[1][2]/3600,stats[1][3]/3600,jobdata[1],run_sum[1]/3600,run_sum[1]/3600/NUM_OF_NANO_NODES,NUM_OF_NANO_NODES))
