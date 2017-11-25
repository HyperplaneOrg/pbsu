#!/usr/bin/env python

# A simple pbs qsub wrapper for submitting job sequences,
# a job that depend on previous job, etc...

import argparse
import subprocess
import sys
import re
import logging
import math

def run_cmd(cargs):
   try:
      logging.debug(cargs)
      sbo = subprocess.check_output(cargs)
      return sbo
   except subprocess.CalledProcessError as sbe: 
      logging.error("%s", str(sbe))
      sys.exit(1)
#run_cmd

def qstat(jid):
   oupt = run_cmd(['qstat', jid])
   logging.debug(oupt)
#qstat

def qsub(pbsfname, depname=None, afermode='afterany'):
   if depname is None:
      oupt = run_cmd(['qsub', pbsfname])
   else:
      qcmd = ['qsub', '-W', "depend=%s:%s" % (afermode, depname), pbsfname]
      oupt = run_cmd(qcmd)
   jid = oupt.strip()
   logging.debug("JobId \"%s\"", jid)
   return jid 
#qstat

def job_chain(jobchain, firstjob=None, aftermode='afterany'):
   jdep = firstjob
   for j in jobchain:
      logging.debug("submitting %s, depends on %s, pbsdepend %s", j, str(jdep), aftermode)
      jdep = qsub(j, jdep, 'afterany')
#job_chain

def main():
   '''entry point'''
   parser = argparse.ArgumentParser()
   parser.add_argument('-c', '--check', help='Check the dependent job.', required=False, action='store_true', default=False)
   parser.add_argument('-v', '--verbose', help='Run in verbose mode', required=False, action='store_true', default=False)
   parser.add_argument('-n', '--concurrent', help='Run <n> jobs concurrently', required=False, type=int, default=1)
   parser.add_argument('depjob', nargs=1, help='The job to depend on. If set to \"first\" use the first job in the jobs arg.')
   parser.add_argument('jobs', nargs='*', help='The job(s) to run. If no jobs are given then the jobs are taken from stdin.')
   args = parser.parse_args()
   isfirst = re.compile(r'first', re.I)

   if args.verbose is True:
      logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s')
   else:
      logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')

   jobs = None
   numjbs = len(args.jobs)
   if numjbs == 0:
      jobs = [ln.strip() for ln in sys.stdin.readlines()] # attempt to take from stdin
      numjbs = len(jobs)
   else:
      jobs = args.jobs

   usefirst = args.depjob[0]
   if isfirst.search(usefirst) is not None:
      usefirst = None
   elif args.check is True:
      qstat(usefirst)
   
   if numjbs < args.concurrent:
      concur = numjbs 
   else:
      concur = args.concurrent

   if concur > 1:
      chunk = int(math.ceil(float(numjbs) / float(concur)))
      slc = [slice(r, r+chunk) if r+chunk <= numjbs else slice(r, numjbs) for r in range(0, numjbs, chunk)]
      jobs = [jobs[s] for s in slc]
   else:
      jobs = [jobs]

   logging.debug("depjob=%s, num jobs=[%d], concurrent=%d", usefirst, numjbs, concur)
   logging.debug("jobs=%s", jobs)

   for j in jobs: 
      job_chain(j, usefirst)
#main

if __name__ == "__main__":
   try:
      sys.exit(main())
   except KeyboardInterrupt as kbe:
      sys.stderr.write("user aborted with a keyboard interrupt...\n")
      sys.exit(1)
#__main__
