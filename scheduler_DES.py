debug = False
#tiny      8:00:00 = 240 minutes
#short  1-00:00:00 = 1440
#defq* 14-00:00:00 =


NANO_TIME_LIMIT = 30
TINY_TIME_LIMIT = 240
SHORT_TIME_LIMIT = 1440
# Total Nodes for CPU jobs = 160
AVAILABLE_NODES = 160
NUM_OF_DEFQ_NODES=32
NUM_OF_SHORT_NODES=32
NUM_OF_TINY_NODES = 48


import sys
import getopt
import pandas as pd
pd.options.mode.chained_assignment = None 
import simpy
import numpy as np
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def usage():
    if rank == 0:
        print("All arguments are required")
        print("\t-m Mode: auto or user")
        print("\t-f Input file name")
        print("\t-d Number of defq nodes")
        print("\t-s Number of short nodes")
        print("\t-t Number of tiny nodes")
        print("\t-n Number of nano nodes")
        print("Example: mpiexec -n 4 python3 scheduler_DES.py -m auto -f $NAME -d 54 -s 26 -n 30  -t 50")
        print("\tThere should be as many mpi processes as there are partitions. In the current implementation mpiexec -n")
    exit(1)

try:
	opts, args = getopt.getopt(sys.argv[1:], "m:f:d:s:t:n:")
except getopt.GetoptError:
    usage()

MODE = None
INPUT_FILE = None
NUM_OF_DEFQ_NODES = None
NUM_OF_SHORT_NODES = None
NUM_OF_TINY_NODES = None
NUM_OF_NANO_NODES = None

for opt, arg in opts:
	if opt in ['-m']:
		MODE = arg
	elif opt in ['-f']:
		INPUT_FILE = arg
	elif opt in ['-d']:
		NUM_OF_DEFQ_NODES = int(arg)
	elif opt in ['-s']:
		NUM_OF_SHORT_NODES = int(arg)
	elif opt in ['-t']:
		NUM_OF_TINY_NODES = int(arg)
	elif opt in ['-n']:
		NUM_OF_NANO_NODES = int(arg)
	elif opt in ['-h']:
		usage()


TOTAL_NUM_OF_NODES = NUM_OF_DEFQ_NODES+NUM_OF_SHORT_NODES+NUM_OF_TINY_NODES+NUM_OF_NANO_NODES
if TOTAL_NUM_OF_NODES != AVAILABLE_NODES:
	if rank == 0:
		print("Number of CPU nodes must equal 160!")
	exit(1)

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

if MODE != 'user' and MODE != 'auto':
	print(sys.argv[1])
	print("Must specify user or auto")
	print("user mode ceased to be useful - avoid using it")
	exit(1)

# Each worker should project out their own dataframe from the master dataframe.
if MODE == 'auto':
	if rank == 0:
		df = df_init[df_init.Timelimit > SHORT_TIME_LIMIT]
	if rank == 1:
		df = df_init[(df_init.Timelimit <= SHORT_TIME_LIMIT) & (df_init.Timelimit > TINY_TIME_LIMIT)]
	if rank == 2:
		df = df_init[(df_init.Timelimit <= TINY_TIME_LIMIT) & (df_init.Timelimit > NANO_TIME_LIMIT)]
	if rank == 3:
		df = df_init[df_init.Timelimit <= NANO_TIME_LIMIT]
elif MODE == 'user':
	if rank == 0:
		print("user mode ceased to be useful - avoid using it")
		df = df_init[df_init.Partition=='defq']
	if rank == 1:
		df = df_init[df_init.Partition=='short']
	if rank == 2:
		df = df_init[df_init.Partition=='tiny']
else: 
	print("Unknown option")
	exit(1)

df.reset_index(drop=True,inplace=True)
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

wait_time  = []
run_time = []

def job(env,JobID,SubmitTime,RunTime,Priority,Partition,Timelimit,InterArrival,NCPUS,node):
    queued = env.now
    with node.request(priority=np.absolute(Priority-MAX_SLURM_PRIO)) as req:
        yield req
        wait_time.append(env.now - queued)
        yield env.timeout(RunTime)
        run_time.append(RunTime)

def run_jobs(env):
    for i in range(NUM_OF_JOBS):
    	yield env.timeout(df['InterArrival'][i]) # Change to waittime at some point?
    	tic = env.now
    	env.process(job(env, df['JobID'][i],df['SubmitTime'][i],df['RunTime'][i],df['Priority'][i],df['Partition'][i],df['Timelimit'][i],df['InterArrival'][i],df['NCPUS'], node=node))    
    	toc = env.now




env = simpy.Environment()
if rank == 0:
	node = simpy.PriorityResource(env, capacity = NUM_OF_DEFQ_NODES)
if rank == 1:
	node = simpy.PriorityResource(env, capacity = NUM_OF_SHORT_NODES)
if rank == 2:
	node = simpy.PriorityResource(env, capacity = NUM_OF_TINY_NODES)
if rank == 3:
	node = simpy.PriorityResource(env, capacity = NUM_OF_NANO_NODES)

env.process(run_jobs(env))
env.run()
#env.run(until=1000)

stats=np.percentile(wait_time, [50, 75, 95])

run_sum = np.sum(run_time)
run_mean = np.average(run_time)
run_med = np.median(run_time)
'''
if rank == 0:
        print("Mode %s, Defq %d, Short %d, Tiny %d, Nano %d :: Short TL %d, Tiny TL %d, Nano TL %d" %(MODE,NUM_OF_DEFQ_NODES,NUM_OF_SHORT_NODES,NUM_OF_TINY_NODES,NUM_OF_NANO_NODES,SHORT_TIME_LIMIT,TINY_TIME_LIMIT,NANO_TIME_LIMIT))
        print("DefQ 50th %.2f 75th %.2f 95th %.2f " % (stats[0]/3600,stats[1]/3600,stats[2]/3600))
        print("Number of Jobs in defq %d, aggregate run time %.2f [hrs], agg run time/node %.2f" %(len(df),run_sum/3600,run_sum/3600/NUM_OF_DEFQ_NODES))
if rank == 1:
        print("Short 50th %.2f 75th %.2f 95th %.2f " % (stats[0]/3600,stats[1]/3600,stats[2]/3600))
        print("Number of Jobs in short %d, aggregate run time %.2f [hrs], agg run time/node %.2f " %(len(df),run_sum/3600,run_sum/3600/NUM_OF_SHORT_NODES))
if rank == 2:
        print("Tiny 50th %.2f 75th %.2f 95th %.2f " % (stats[0]/3600,stats[1]/3600,stats[2]/3600))
        print("Number of Jobs in tiny %d, aggregate run time %.2f [hrs], agg run time/node %.2f " %(len(df),run_sum/3600,run_sum/3600/NUM_OF_TINY_NODES))
if rank == 3:
        print("Nano 50th %.2f 75th %.2f 95th %.2f " % (stats[0]/3600,stats[1]/3600,stats[2]/3600))
        print("Number of Jobs in nano %d, aggregate run time %.2f [hrs], agg run time/node %.2f " %(len(df),run_sum/3600,run_sum/3600/NUM_OF_NANO_NODES))
'''
comm.Barrier()
stats=comm.gather(stats, root=0)
jobdata=comm.gather(len(df),root=0)
run_sum=comm.gather(run_sum,root=0)

total_jobs = comm.reduce(len(df), op=MPI.SUM,root=0)
if rank == 0:
	print("Mode %s, Defq %d, Short %d, Tiny %d, Nano %d :: Short TL %d, Tiny TL %d, Nano TL %d" %(MODE,NUM_OF_DEFQ_NODES,NUM_OF_SHORT_NODES,NUM_OF_TINY_NODES,NUM_OF_NANO_NODES,SHORT_TIME_LIMIT,TINY_TIME_LIMIT,NANO_TIME_LIMIT))
	print("Total Jobs %d" %(total_jobs))
	print("Part 50th  75th   95th  NumJobs  agg_runtime agg_runtime/node")
	print("Defq %.2f %.2f %.2f %d %.2f %.2f" % (stats[0][0]/3600,stats[0][1]/3600,stats[0][2]/3600,jobdata[0],run_sum[0]/3600,run_sum[0]/3600/NUM_OF_DEFQ_NODES))
	print("Short %.2f %.2f %.2f %d %.2f %.2f" % (stats[1][0]/3600,stats[1][1]/3600,stats[1][2]/3600,jobdata[1],run_sum[1]/3600,run_sum[1]/3600/NUM_OF_SHORT_NODES))
	print("Tiny %.2f %.2f %.2f %d %.2f %.2f" % (stats[2][0]/3600,stats[2][1]/3600,stats[2][2]/3600,jobdata[2],run_sum[2]/3600,run_sum[2]/3600/NUM_OF_TINY_NODES))
	print("Nano %.2f %.2f %.2f %d %.2f %.2f" % (stats[3][0]/3600,stats[3][1]/3600,stats[3][2]/3600,jobdata[3],run_sum[3]/3600,run_sum[3]/3600/NUM_OF_NANO_NODES))
