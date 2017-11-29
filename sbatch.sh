#!/bin/bash

#SBATCH -t 10:00
#SBATCH -N 1
RESOURCEDIR="/srv/os3/job"
JOBDIR="/srv/os3/job-${SLURM_JOB_ID}"
if mkdir "$JOBDIR"
then
  if cd "$JOBDIR"
  then
    srun "$RESOURCEDIR/srun.sh"
  else
    echo "Could not cd into the job directory"
    exit 1
  fi
fi
