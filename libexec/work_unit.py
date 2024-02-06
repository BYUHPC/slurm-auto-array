#!/usr/bin/env python3

import os, socket, getpass, re, shlex, sys, subprocess

"""
work_unit.py open_mode outfile_format errfile_format "work_unit_id command 'arg 1' [arg2 ...]"

Arguments are all passed as a single argument to be split by shlex, since that's how they exist in the input file

This script is not meant to be run manually.
"""

def outfile_name(format, work_unit_id, command):
    if '\\' in format:
        return re.sub("\\", "", format)
    master_job_id = os.environ["SLURM_ARRAY_JOB_ID"]
    job_name = os.environ["SLURM_JOB_NAME"]

    # Fixed replacements
    replacements = {
        "%" : "%",
        "a" : str(work_unit_id),
        "A" : master_job_id,
        "N" : socket.gethostname().split(".", 1)[0],
        "u" : getpass.getuser(),
        "x" : job_name
    }

    # Argument based replacements
    for i, arg in enumerate(command):
        replacements[str(i)] = arg

    print("FORMAT: ", format)
    # Perform the replacements and return the file name
    for pattern, replacement in replacements.items():
        for n in re.findall(f"%(\d+)?{pattern}", format):
            if n and replacement.isdigit():
                format = re.sub(f"%{n}{pattern}", replacement.zfill(int(n)), format)
            else:
                format = re.sub(f"%{pattern}", replacement, format)
    return format



def parse(args):
    outfile_format = args[0]
    errfile_format = args[1]
    open_mode = args[2]
    command = args[3:-1]
    work_unit_id, remaining_args = args[-1].split(" ", 1)
    command += shlex.split(remaining_args)
    return outfile_format, errfile_format, open_mode, work_unit_id, command



def main(args=sys.argv[1:]):
    # "Parse"
    outfile_format, errfile_format, open_mode, work_unit_id, command = parse(args)
    # Redirect stderr and stdout to the appropriate file(s)
    with open(outfile_name(outfile_format, work_unit_id, command), open_mode) as out, \
         open(outfile_name(errfile_format, work_unit_id, command), open_mode) as err:
        # First try a command within this directory
        try:
            subprocess.run(["./" + command[0]] + command[1:], stdout=out, stderr=err)
        except FileNotFoundError:
            # Next, try a command in PATH
            try:
                subprocess.run(command, stdout=out, stderr=err)
            # If neither worked, print an error
            except FileNotFoundError:
                print(f"Could not find command '{command[0]}' in current directory or $PATH", file=sys.stderr)
                exit(1)



if __name__ == "__main__":
    main()
