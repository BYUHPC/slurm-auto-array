#!/bin/bash

# This script is meant to be submitted as a job by slurm-auto-array
# It uses GNU Parallel to split work; if that ends up not working well it can use srun instead
# Work is split up in stripes, not chunks--e.g. one instance might get arguments 1,4,7 while the next gets 2,5,8
# The actual work units are run by work_unit.py in this same directory

# "Parse"
infile="$1-$SLURM_ARRAY_TASK_ID.in"
shift
delimiter="$1"
shift

# Determine how many tasks should be run on which nodes
ssh_login_file="$(mktemp)"
trap 'rm "$ssh_login_file" "$infile"' EXIT
paste -d '/' <(perl -pe 's/(\d+)\(x(\d+)\)/substr("$1,"x$2,0,-1)/ge' <<< $SLURM_TASKS_PER_NODE | tr ',' '\n') \
             <(scontrol show hostnames) > "$ssh_login_file"

# Launch workers
parallel --arg-file "$infile" \
         --delimiter "$delimiter" \
         --env _ \
         --quote \
         --jobs "$SLURM_NTASKS" \
         --ssh "ssh -o ServerAliveInterval=300" --sshloginfile "$ssh_login_file" \
         --workdir . \
         python3 "$@" {} & wait
