#!/usr/bin/env bash

# CONDITIONS FOR TESTING:
# Slurm is installed and working, and you're able to submit small jobs
# Bats is installed
# Your home directory is accessible via compute nodes
# You have parallel installed and in PATH

# THINGS TO TEST
# Precedence of environment variables vs config files vs command line arguments
# Split into the right amount of array tasks
# Job actually runs
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
}

teardown_file() {
    rm -rf "$SAA_TESTING_DIR"
}

@test "slurm-auto-array exits 0 when '--help' is supplied" {
    slurm-auto-array --help
}

@test "basic job submission works" {
    local ARGS="$(echo -e 'A\nB\nC')"
    submit_job "$ARGS" --wait -U 1,0,1G,1 -l "$SAA_TESTING_DIR/basic-test.log" -o "$SAA_TESTING_DIR/basic-test-%a.out" -- echo arguments supplied:
    test "$(cat "$SAA_TESTING_DIR/basic-test-3.out")" = "arguments supplied: C"
}
