#!/usr/bin/env python

"""
Start a dask.distributed cluster on a UGE scheduler, keep it alive as need be,
and delete it when the time comes.
"""

import subprocess
import logging
import socket
import time
import signal
import xml.etree.ElementTree as ET
from textwrap import dedent

setup = """
eval $(/cvmfs/icecube.opensciencegrid.org/py2-v2/setup.sh)
unset PYTHONPATH
. ~/scratch/software/sncosmo/bin/activate
"""

def qsub(*args):
	"""
	Submit a job
	"""
	std_args = ('-V', '-j', 'y', '-o', '/dev/null')
	pipe = subprocess.Popen(('qsub',) + args, stdout=subprocess.PIPE)
	stdout, _ = pipe.communicate()
	if pipe.returncode == 0:
		return stdout.split(' ')[2]

def qsub_script(script, uge_args=[]):
	pipe = subprocess.Popen(['qsub',] + list(uge_args), stdout=subprocess.PIPE, stdin=subprocess.PIPE)
	stdout, _ = pipe.communicate(dedent(script))
	if pipe.returncode == 0:
		return stdout.split(' ')[2]
	

def job_exists(jobid):
	"""
	Does the job still exist, or has it been killed?
	"""
	devnull = open('/dev/null', 'w')
	return subprocess.call(['qstat', '-j', jobid], stdout=devnull, stderr=devnull) == 0

def count_tasks(jobid):
	"""
	Count the number of live tasks in a job array
	"""
	pipe = subprocess.Popen(['qstat', '-xml', '-j', jobid], stdout=subprocess.PIPE)
	stdout, _ = pipe.communicate()
	if pipe.returncode != 0:
		return 0
	root = ET.fromstring(stdout)
	
	tasks = root.find('.//JB_ja_tasks')
	if tasks is None:
		return 0
	return len(tasks)

def qdel(jobs):
	"""
	Delete jobs
	"""
	devnull = open('/dev/null', 'w')
	jobs = [j for j in jobs if isinstance(j, str)]
	subprocess.call(['qdel'] + list(jobs))
	# subprocess.call(['qdel'] + list(jobs), stdout=devnull, stderr=devnull)
	

def executing_host(jobid):
	"""
	Return the hostname where *jobid* is running
	"""
	for i in xrange(100):
		time.sleep(5)
		pipe = subprocess.Popen(('qstat', '-j', jobid, '-xml'), stdout=subprocess.PIPE)
		stdout, _ = pipe.communicate()
		root = ET.fromstring(stdout)
		
		host = root.find('.//JG_qhostname')
		if host is not None:
			return host.text

class ClusterNanny(object):
	def __init__(self, nworkers, interval=300):
		super(ClusterNanny, self).__init__()
		self.logger = logging.getLogger('ClusterNanny')
		self.scheduler_id = None
		self.scheduler_host = None
		self.worker_ids = set()
		self.nworkers = nworkers
		self.interval = interval

	def submit_scheduler(self):
		# set up enviroment, forward ports from submitting host, and run scheduler
		script = """#!/bin/sh
		{setup}
		ssh -R8786:localhost:8786 -R9786:localhost:9786 -R8787:localhost:8787 -N -o KeepAlive=yes -o ServerAliveInterval=55 {remote_host}&
		dask-scheduler
		""".format(setup=setup, remote_host=socket.gethostname())
		
		jid = qsub_script(script,
		           [
		           '-N', 'dask-scheduler',
		           '-l', 'h_rt=11:59:00',
		           '-l', 'h_rss=4G',
		           '-j', 'y', #'-o', '/dev/null',
		           ])
		self.scheduler_host = executing_host(jid)
		self.logger.info('Scheduler executing on {}:8786'.format(self.scheduler_host))
		self.scheduler_id = jid
	
	def submit_workers(self, nworkers):
		script = """#!/bin/sh
		{setup}
		dask-worker --nthreads 1 {scheduler}:8786
		""".format(setup=setup, scheduler=self.scheduler_host)
		
		jid = qsub_script(script,
		    [
		    '-N', 'dask-worker',
		    '-t', '1-%d' % (nworkers),
		    # '-l', 'h_rss=4G',
		    '-j', 'y', '-o', '/dev/null',
		    ])
		self.worker_ids.add(jid)
	
	def stop_cluster(self):
		if self.scheduler_id is not None:
			self.logger.info('Stopping cluster')
			qdel(list(self.worker_ids) + [self.scheduler_id,])
	
	def start_cluster(self, nworkers):
		self.stop_cluster()
		self.submit_scheduler()
		self.submit_workers(nworkers)
	
	def run(self):
		self.start_cluster(self.nworkers)
		
		while True:
			try:
				time.sleep(self.interval)
			except KeyboardInterrupt:
				self.stop_cluster()
				return
			
			try:
				# check whether the scheduler is still alive
				if not job_exists(self.scheduler_id):
					self.logger.warn("Scheduler has died! Restarting cluster...")
					self.start_cluster(self.nworkers)
				else:
					# otherwise, check if any workers have expired
					nworkers = 0
					for group in self.worker_ids:
						n = count_tasks(group)
						if n == 0:
							self.worker_ids.remove(group)
						self.logger.info("Job {} has {} tasks".format(group, n))
						nworkers += n
					if nworkers < self.nworkers:
						self.logger.info("Submitting {} workers".format(self.nworkers-nworkers))
						self.submit_workers(self.nworkers-nworkers)
			except:
				self.stop_cluster()
				raise

if __name__ == "__main__":
	from argparse import ArgumentParser
	parser = ArgumentParser(description=__doc__)
	parser.add_argument('-n', '--nworkers', type=int, default=10)
	opts = parser.parse_args()
	
	import logging
	logging.basicConfig(level='INFO')
	
	cluster = ClusterNanny(opts.nworkers)
	cluster.run()
