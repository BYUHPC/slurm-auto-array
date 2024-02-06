#!/usr/bin/env bats

# CONDITIONS FOR TESTING:
# Slurm is installed and working, and you're able to submit small jobs
# Bats is installed
# Your home directory is accessible via compute nodes
# You have parallel installed and in PATH

# THINGS TO TEST
# Precedence of environment variables vs config files vs command line arguments
# Correct rejection of bad arguments
# Output files are correct and named correctly
# Changing deliminters works
# -- COMMAND works in various configurations--none, just the command, the command with arguments, etc.
# -U and -T properly supersede other arguments
# Lots more, this is just off the top of my head

submit_job() {
    local ARGS="$1"
    shift # subesequent arguments passed to slurm-auto-array
    echo "$ARGS" | slurm-auto-array --parsable "$@"
}

setup_file() {
    local SAA_DIR="$(dirname "$(dirname "$(realpath "$BATS_TEST_FILENAME")")")"
    export PATH="$SAA_DIR/bin:$PATH"
    export SAA_TESTING_DIR="$(mktemp -d ~/.cache/saa-test-XXX)"
    cd "$SAA_TESTING_DIR"
}

teardown_file() {
    cd
    rm -rf "$SAA_TESTING_DIR"
}





@test "slurm-auto-array exits 0 when '--help' is supplied" {
    slurm-auto-array --help
}



@test "basic job submission works" {
    local ARGS="$(echo -e 'A\nB\nC')"
    submit_job "$ARGS" --wait -U 1,0,1G,1 -l "basic-test.log" -o "basic-test-%a.out" -- echo arguments supplied:
    test "$(cat "$SAA_TESTING_DIR/basic-test-3.out")" = "arguments supplied: C"
}



@test "only the minimum necessary resources are used given the work unit size and count" {
    check_submission_size() {
        # Parse
        local N="$1"                                  # number of work units
        local TASKS="$2"                              # expected task count
        local U="$3"                                  # work unit size specification
        local A="$4"                                  # expected array task size
        local CPUS="$(awk -F, '{print $1}' <<< "$A")" # expected CPU count
        local MEM="$( awk -F, '{print $3}' <<< "$A")" # expected memory request
        local TIME="$(awk -F, '{print $4}' <<< "$A")" # expected time limit

        # Submit job as requested, then immediately cancel since it isn't to be run
        local args="$(seq "$N")"
        job_id="$(submit_job "$args" --hold -U "$U" -- echo)" # can't be local since we want the exit status
        [[ $? -eq 0 ]] || return 1 # return 1 on submissoin failure
        local scontrol_output="$(scontrol show job "$job_id" 2>&1)"
        scancel "$job_id"

        # Actual values
        scontrol_get() {
            grep -oP "$1=\K$2" <<< "$scontrol_output"
        }
        local tasks="$(scontrol_get ArrayTaskId '[0-9-]+' | awk -F- '{print $NF}')"
        local cpus="$(scontrol_get NumCPUs '\d+')"
        local mem="$(scontrol_get mem '\d+[KMGT]')"
        local timelim="$(scontrol_get TimeLimit '[0-9:]+')"

        # See if everything matches expectations
        local problem=""
        [[ "$tasks"   = "$TASKS" ]] || problem="$problem|array task counts ($TASKS and $tasks) don't match|"
        [[ "$cpus"    = "$CPUS"  ]] || problem="$problem|CPU counts ($CPUS and $cpus) don't match|"
        [[ "$mem"     = "$MEM"   ]] || problem="$problem|memory requests ($MEM and $mem) don't match|"
        [[ "$timelim" = "$TIME"  ]] || problem="$problem|time limits ($TIME and $timelim) don't match|"

        # Return 1 if anything didn't match
        if [[ ! -z "$problem" ]]; then
            echo "ARGUMENTS: $@"
            echo "PROBLEM:   $problem"
            scontrol show job "$job_id"
            return 2 # return 2 on incorrect results
        fi
    }

    # SAA environment variables
    export SAA_MAX_ARRAY_TASKS=4
    export SAA_DEFAULT_ARRAY_TASK_SIZE=4,1,4G,00:05:00
    export SAA_MAX_ARRAY_TASK_SIZE=10,1,12G,00:30:00

    # Tests
    check_submission_size 1   1   1,0,1M,00:01:00   1,0,1M,00:01:00
    check_submission_size 10  1   1,0,10M,00:01:00  2,0,20M,00:05:00
    check_submission_size 10  2   1,0,4G,00:01:00   1,0,4G,00:05:00
    check_submission_size 120 4   1,0,1G,00:09:00   10,0,10G,00:27:00
    # These two are expected to fail--the first needs too many tasks, the second too big a work unit
    { check_submission_size 121 4   1,0,1G,00:09:00   10,0,10G,00:27:00 || test $? -eq 1; }
    { check_submission_size 1   1   11,0,1G,00:01:00  11,0,1G,00:01:00  || test $? -eq 1; }
}



@test "outfile name formatting works" {
    FORMAT='test-%%-%a-%A-%N-%u-%x-%1-%2.out'
    job_id="$(submit_job "$(seq 4 | awk '{print argument, $1}')" --verbose -n 1 --mem 256m -t 1 --wait -o "$FORMAT" --job-name oftest -- echo argument)"
    job_host="$(scontrol show job "$job_id" | grep -oP '\sNodeList=\K\S+')"
    for i in {1..4}; do
        filename="test-%-$i-$job_id-$job_host-$USER-oftest-argument-$i.out"
        ls "$filename"
        test "$(cat "$filename")" = "argument $i"
    done
    # TODO: test that when you specify %3 but there are only two arguments on the command line, it just stays as '%3' (and document that's what happens)
}
