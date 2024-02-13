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
# Delimiters
# Lots more, this is just off the top of my head

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



# Submit a job with slurm-auto-array and print the job's ID
submit_job() {
    # $1: the arguments to pour into slurm-auto-array (newline-delimited string)
    # $2...: arguments to pass to slurm-auto-array
    local ARGS="$1"
    shift # subesequent arguments passed to slurm-auto-array
    echo "$ARGS" | slurm-auto-array --parsable "$@"
}



# Extract a value from the output of `scontrol show job`
scontrol_get() {
    # $1: the job ID
    # $2: the value to search for
    # $3: the regular expression to match
    local job_id="$1"
    local value="$2"
    local pattern="$3"

    # Make sure the output is in the cache
    declare -gA scontrol_show_cache # it can be declared multiple times to no ill effect
    if [[ -z "${scontrol_show_cache["$job_id"]}" ]]; then
        local scontrol_output="$(scontrol show job "$job_id")"
        scontrol_show_cache["$job_id"]="$scontrol_output"
    fi

    # Grep for the given value and regex
    grep -oP "$value=\K$pattern" <<< "${scontrol_show_cache["$job_id"]}"
}





@test "slurm-auto-array exits 0 when '--help' is supplied" {
    slurm-auto-array --help
}



@test "basic job submission works" {
    local ARGS="$(echo -e 'A\nB\nC')"
    submit_job "$ARGS" --wait -U 1,0,1G,1 -l "basic-test.log" -o "basic-test-%a.out" -- echo arguments supplied:
    ls "$SAA_TESTING_DIR"
    echo "CONTENTS: '$(cat "$SAA_TESTING_DIR/basic-test-2.out")'"
    test "$(cat "$SAA_TESTING_DIR/basic-test-2.out")" = "arguments supplied: C"
}



@test "only the minimum necessary resources are used given the work unit size and count, and config works correctly" {
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
        work_unit_size=()
        [[ -z "$U" ]] || work_unit_size+=(-U "$U")
        job_id="$(submit_job "$args" --hold "${work_unit_size[@]}" -- echo)" # can't be local since we want the exit status
        [[ $? -eq 0 ]] || return 1 # return 1 on submissoin failure
        scancel "$job_id"

        # Actual values
        local tasks="$(scontrol_get $job_id ArrayTaskId '[0-9-]+' | awk -F- '{print $NF+1}')"
        local cpus="$(scontrol_get $job_id NumCPUs '\d+')"
        local mem="$(scontrol_get $job_id mem '\d+[KMGT]')"
        local timelim="$(scontrol_get $job_id TimeLimit '[0-9:]+')"

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

    # Unset relevant environment variables
    unset SAA_MAX_ARRAY_TASKS SAA_DEFAULT_WORK_UNIT_SIZE SAA_DEFAULT_ARRAY_TASK_SIZE SAA_MAX_ARRAY_TASK_SIZE

    # Create a config file
    export SAA_CONFIG_FILE="$(mktemp)" # node-local should be okay
    echo 'SAA_MAX_ARRAY_TASKS=4
SAA_DEFAULT_WORK_UNIT_SIZE=1,0,1M,00:01:00
SAA_DEFAULT_ARRAY_TASK_SIZE=1,1,4G,00:10:00' > "$SAA_CONFIG_FILE"

    # Set environment variables (higher precedence than config)
    export SAA_MAX_ARRAY_TASK_SIZE=10,1,12G,00:30:00
    export SAA_DEFAULT_ARRAY_TASK_SIZE=4,1,4G,00:05:00

    # Tests
    check_submission_size 1   1   ''                1,0,1M,00:01:00   # use default work unit size
    check_submission_size 10  1   1,0,10M,00:01:00  2,0,20M,00:05:00
    check_submission_size 10  2   1,0,4G,00:01:00   1,0,4G,00:05:00
    check_submission_size 120 4   1,0,1G,00:09:00   10,0,10G,00:27:00
    # These two are expected to fail--the first needs too many tasks, the second too big a work unit
    { check_submission_size 121 4   1,0,1G,00:09:00   10,0,10G,00:27:00 || test $? -eq 1; }
    { check_submission_size 1   1   11,0,1G,00:01:00  11,0,1G,00:01:00  || test $? -eq 1; }

    # Clean up
    rm "$SAA_CONFIG_FILE"
}



@test "outfile name formatting works" {
    FORMAT='testof-%%-%a-%A-%N-%u-%x-%1-%2.out'
    job_id="$(submit_job "$(seq 4 | awk '{print argument, $1}')" --verbose -n 1 --mem 256m -t 1 --wait -o "$FORMAT" --job-name oftest -- echo argument)"
    job_host="$(scontrol show job "$job_id" | grep -oP '\sNodeList=\K\S+')"
    ls
    for i in {1..4}; do
        filename="testof-%-$((i-1))-$job_id-$job_host-$USER-oftest-argument-$i.out"
        test "$(cat "$filename")" = "argument $i"
    done
    ls testof-%-*-$job_id-* | wc -l
    test "$(ls testof-%-*-$job_id-* | wc -l)" = 4
    # TODO: test that when you specify %3 but there are only two arguments on the command line, it just stays as '%3' (and document that's what happens)
}



@test "#SBATCH/#SAA argument parsing works" {
    # Make sure that both #SBATCH and #SAA arguments are heeded
    echo "#!/bin/bash
#SBATCH -t 2 --mem 16M
#SAA -n 4
hostname" > command.sh
    job_id="$(submit_job 1 --hold -- command.sh)"
    scontrol show job "$job_id"
    test "$(scontrol_get $job_id NumCPUs '\d+')"       = 4
    test "$(scontrol_get $job_id mem '\d+[KMGT]')"     = 16M
    test "$(scontrol_get $job_id TimeLimit '[0-9:]+')" = 00:02:00
    scancel "$job_id"

    # Check that forbidden #SBATCH arguments are rejected
    bad_infile="$(mktemp)"
    echo '#!/bin/bash
#SBATCH --array 1-2
echo "$@' > "$bad_infile"
    { PATH="$(dirname "$bad_infile"):$PATH" submit_job 1 -- "$(basename "$bad_infile")" || test $? -eq 1; }
    rm "$bad_infile"
}



@test "--arg-file works" {
    argfile1="$(mktemp ./arg-file-test-XXXX)"
    argfile2="$(mktemp ./arg-file-test-XXXX)"
    seq 3 > "$argfile1"
    (echo a; echo b) > "$argfile2"

    # Single argument file
    submit_job "$(echo a b; echo c)" --arg-file "$argfile1" --wait -o testaf-%a.out -- bash -c 'while read arg; do echo "$arg"; done'
    test "$(cat testaf-0.out)" = "$(echo a b; echo c)"

    # Multiple argument files
    submit_job "X" -a "$argfile1" --wait -o testaf-%1-%2.out -a "$argfile2" -- echo
    ls
    for i in $(seq 1 3); do
        for letter in a b; do
            test "$(cat testaf-$i-$letter.out)" = "$i $letter"
        done
    done
}