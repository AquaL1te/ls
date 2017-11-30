#!/bin/bash

#SBATCH -t 30:00
#SBATCH -N 1
resourcedir="/srv/os3/job"
jobdir="/srv/os3/job-${SLURM_JOB_ID}"
if mkdir "$jobdir"
then
  if cd "$jobdir"
  then
    srun "$resourcedir/srun.sh"
  else
    echo "Could not cd into the job directory"
    exit 1
  fi
fi
