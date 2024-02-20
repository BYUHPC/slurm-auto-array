#!/usr/bin/env python3

import os, socket, getpass, re, shlex, sys, subprocess

"""
work_unit.py open_mode outfile_format errfile_format "work_unit_id command 'arg 1' [arg2 ...]"

Arguments are all passed as a single argument to be split by shlex, since that's how they exist in the input file

This script is not meant to be run manually.
"""



def read_to_delimiter(file_path, start_pos, delimiter):
    with open(file_path, 'rb') as file:
        # Seek to the specified start position
        file.seek(start_pos)

        buffer = b''
        chunk_size = 4096 # probably won't use this much, but it's not like reading less is likely to affect spped

        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break  # End of file reached

            buffer += chunk
            delimiter_pos = buffer.find(delimiter.encode())

            if delimiter_pos != -1:
                return buffer[:delimiter_pos].decode()

        # Return whatever is available if the delimiter is not found
        return buffer.decode()



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

    # Perform the replacements and return the file name
    for pattern, replacement in replacements.items():
        for n in re.findall(f"%(\d+)?{pattern}", format):
            if n and replacement.isdigit():
                format = re.sub(f"%{n}{pattern}", replacement.zfill(int(n)), format)
            else:
                format = re.sub(f"%{pattern}", replacement, format)
    return format



def parse(args):
    # Get arguments from command line
    arg_file_subdir = args[0]
    outfile_format = args[1]
    errfile_format = args[2]
    open_mode = args[3]
    exterior_delimiter = args[4]
    interior_delimiter = args[5]
    command = args[6:-1]
    work_identifier = args[-1]

    # Find work unit ID
    parsed_identifier = work_identifier.split(",")
    work_unit_id = int(parsed_identifier[0])

    # Iterate over argument files, getting arguments based on the positions specified by the work identifier
    arg_files = [os.path.join(arg_file_subdir, f) for f in os.listdir(arg_file_subdir) if re.match(r"args.\d+.\d+", f)]
    for arg_file, position in zip(arg_files, parsed_identifier[1:]):
        raw_args = read_to_delimiter(arg_file, int(position), exterior_delimiter)
        if interior_delimiter == "shlex":
            command += shlex.split(raw_args)
        else:
            command += raw_args.strip(interior_delimiter).split(interior_delimiter)

    # Name of the file carrying stdin
    stdinfile = os.path.join(arg_file_subdir, "stdin")

    # Get output and error file actual names given format
    outfile = outfile_name(outfile_format, work_unit_id, command)
    errfile = outfile_name(errfile_format, work_unit_id, command)

    return stdinfile, outfile, errfile, open_mode, command



def main(args=sys.argv[1:]):
    # Parse
    stdin, outfile, errfile, open_mode, command = parse(args)

    # Redirect stderr and stdout to the appropriate file(s)
    with open(outfile, open_mode) as out, \
         open(errfile, open_mode) as err:
        # First try a command within this directory
        opened_stdin = subprocess.DEVNULL if stdin == "/dev/null" else open(stdin)
        try:
            subprocess.run(["./" + command[0]] + command[1:], stdout=out, stderr=err, stdin=opened_stdin)
        except FileNotFoundError:
            # Next, try a command in PATH
            try:
                subprocess.run(command, stdout=out, stderr=err, stdin=opened_stdin)
            # If neither worked, print an error
            except FileNotFoundError:
                print(f"Could not find command '{command[0]}' in current directory or $PATH", file=sys.stderr)
                exit(1)



if __name__ == "__main__":
    main()
