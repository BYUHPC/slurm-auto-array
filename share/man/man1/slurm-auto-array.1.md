---
title: SLURM-AUTO-ARRAY
section: 1
header: BYU Office of Research Computing
---



## NAME

`slurm-auto-array` - Slurm job array automation script



## SYNOPSIS

`slurm-auto-array` [`-h`] [`-V`] [`--delimiter D`] [`-n N`] [`-G N`]
[`-m N{K|M|G}` | `--mem-per-cpu N{K|M|G}` | `--mem-per-gpu N{K|M|G}`]
[`-t D-HH:MM:SS`] [`-U CPUs,GPUs,mem,time`] [`-T CPUs,GPUs,mem,time`]
[`-l x.log`] [`-o x.out`] [`-e x.err`] [`-v`] [`--dry-run`] [`-- COMMAND [args...]`]
[(`::: args` | `:::+ args` | `:::: argfiles` | `::::+ argfiles`)...]

You can also pass most arguments that `sbatch` takes, for instance to specify a QOS.



## DESCRIPTION

`slurm-auto-array` automates and optimizes the submission of Slurm job arrays, aggregating small units of work into
sets of tasks of configurable resource use and duration.

The commands that are run by these work units can be specified in two ways: an arbitrary number of arguments that are
common to all tasks can optionally be given after '`--`' on the `slurm-auto-array` command line, and multiple lines of
differing arguments can be given on stdin. These lines of arguments on stdin are parsed by Python's `shlex`, so the
rules for quoting are what you probably expect. If no arguments are given after `--` on the command line, the lines of
input themselves will be executed.

As an example, this command:

```bash
echo "1 'A a'
      2 'B b'
      3 'C c'" | slurm-auto-array --output job-%a.txt \
                 -- python3 -c "from sys import argv
                                print(f'First: {argv[1]}')
                                print(f'Second: {argv[2]}')"
```

...would produce three files, with the first, `job-1.txt`, containing:

```
First: 1
Second: a b
```

`job-2.txt` and `job-3.txt` would contain similar text, with the numbers and characters swapped appropriately.

Like `parallel`, `:::`, `:::+`, `::::`, and `::::+` can be used to specify arguments rather than stdin--the following
examples will produce equivalent output to the one above:

```bash
slurm-auto-array -o job-%a.txt -- python3 printargs.py :::  1 2 3    :::+ 'A a' 'B b' 'C c'
slurm-auto-array -o job-%a.txt -- python3 printargs.py :::: <(seq 3) :::+ 'A a' 'B b' 'C c'
```

...given that `printargs.py` contains what was run by `python3` in the first example.

In order to reduce strain on the scheduler and maximize throughput, if your command doesn't need much time or many
resources, multiple instances of the command may be aggregated into one array task. For instance, if you submit with
`--time=02:00:00 --ntasks=2 --mem=4G`, each array task may, depending on your configuration, run 4 instances of your
command (hence **work units**) at a time for 12 hours, for a total of 24 instances per array task. This behavior can be
changed by the `--per-task-*` flags.

Some commands are extremely unpredictable in the amount of time and resources they require--it can be hard to predict
whether 5 minutes or 2 hours will be needed for any given argument. Since the arguments that cause unusually high
resource demands are often clustered together, `slurm-auto-array` stripes work across array tasks rather than
chunking--if you have 100 units of work that fit into 5 job array tasks, the first task will run the command on
arguments 1,6,11,16,..., the second on arguments 2,7,12,17,..., etc.

In addition to command line arguments, `#SBATCH` flags will be parsed if an `sbatch` script is supplied as the command.
`#SAA` flags will be treated identically by `slurm-auto-array`, allowing you to add `slurm-auto-array`-specific flags to
job scripts that will also be used with `sbatch`.

You'll need to run `parallel --citation; parallel --record-env` in a clean environment before using `slurm-auto-array`.



## OPTIONS

`-h`, `--help`

:   Print a help message and exit.

`-V`, `--version`

:   Show the version number and exit.

`-v`, `--verbose`

:   Print verbose output.

`--dry-run`

:   Don't submit; print what would have been done.

`-n N`, `--ntasks N`

:   Number of CPUs required by each unit of work.

`-G N`, `--gpus N`

:   Number of GPUs required by each unit of work.

`-m N{K|M|G}`, `--mem N{K|M|G}`

:   Memory required by each unit of work; mutually exclusive with `--mem-per-cpu` and `--mem-per-gpu`.

`--mem-per-cpu N{K|M|G}`

:   Memory required by each CPU (default: 2G); mutually exclusive with `--mem` and `--mem-per-gpu`.

`--mem-per-gpu N{K|M|G}`

:   Memory required by each GPU; mutually exclusive with `--mem` and `--mem-per-cpu`.

`-t D-HH:MM:SS`, `--time D-HH:MM:SS`

:   Time required by each unit of work; see **sbatch(1)** for formatting.

`-U CPUs,GPUs,mem,time`, `--work-unit-size CPUs,GPUs,mem,time`

:   Allocation size of each work unit, with respective formats those of -n, -G, -m, and -t (1,0,1G,1:00:00 by default).
    `-n`, `-G`, `--mem-per-cpu`, and `-t` have higher precedence if specified.

`-T CPUs,GPUs,mem,time`, `--array-task-size CPUs,GPUs,mem,time`

:   Desired allocation size of each array task, with the same format as -U (4,1,16G,6:00:00 by default); `-T` is ignored
    if it isn't sufficiently large to allow the job array to submit.

`-a argfile, --arg-file argfile`

:   Use `argfile` as input rather than stdin. Can be specified multiple times, in which case all combinations of inputs
    from each argument file will be used. If specified, `stdin` will be passed to the work unit commands.

`-l output.log`, `--logfile output.log`

:   `slurm-auto-array` log file. Setting to `/dev/null` will suppress all output, including output and error files
    (default `slurm-auto-array-%A.log`).

`-o output.out`, `--output output.out`

:   File (with optional formatting) to which to write stdout for each array task; only a subset of sbatch's formatting
    options are supported, namely `%%`, `%a`, `%A`, `%N`, `%u`, and `%x`. You can also specify `%0` to replace the
    command being run, `%1` to replace its first argument, `%2` to replace the second, etc. See sbatch(1) (default
    `slurm-auto-array-%A_%a.out`).

`-e output.err`, `--error output.err`

:   Analogous to `-o`, but for stderr; defaults to the file specified by `-o`.

`--open-mode append|truncate`

:   Open log, output, and error files in the specified mode; see sbatch(1) (default truncate).

`--delimiter D, --exterior-delimiter D`

:   The string or regular expression separating work unit command lines (default: newline). As an example, this could be
    `\0` if you were using null-delimited sets of arguments.

`--interior-delimiter D`

:   The string or regular expression separating arguments within a work unit's command line. Python's `shlex` is used by
    default, so you can quote arguments as you would probably expect.

`::: arg(s)`

:   Run the given command on each of the arguments that follow `:::`. Specified after the command. Multiple sets of
    `:::` can be specified, in which case the arguments will be crossed. For example,
    `slurm-auto-array -- echo ::: a b c ::: 1 2` will print every combination of the letter and number arguments
    (`a 1`, `a 2`, `b 1`, ...), for six total work units. If this is specified, stdin will be passed to the work unit
    command rather than being used as arguments.

`:::+ arg(s)`

:   Behaves similarly to `:::`, but pairs its arguments with the previous arguments rather than crossing them. For
    example, `slurm-auto-array -- echo ::: a b c :::+ 1 2 3 4` will print `a 1`, `b 2`, and `c 3`. If the argument lists
    are of different length, the longer list is truncated.

`:::: argfile(s)`

:   Similar to `:::`, but using argument files (see `--arg-file`) rather than raw arguments.

`::::+ argfile(s)`

:   `::::+` is to `::::` as `:::+` is to `:::`.

Since `slurm-auto-array` is a thin wrapper over `sbatch`, any argument that `slurm-auto-array` doesn't recognize is
passed directly to `sbatch`; this means that you can specify constraints, partitions, job names, etc. A few `sbatch`
arguments (those that would interfere with the correct splitting up of work) are disallowed, though, namely: `-a`,
`--array`, `--cpus-per-gpu`, `--cpus-per-task`, `--gpus-per-node`, `--gpus-per-socket`, `--gpus-per-task`,
`--ntasks-per-core`, `--ntasks-per-node`, and `--ntasks-per-socket`.

`#SBATCH` directives within submission scripts are also parsed; `#SAA` directives are treated identically to allow
flags only used for `slurm-auto-array` to be safely included.



## CONFIGURATION

By default, `slurm-auto-array` will find its configuration in `$HOME/.config/slurm-auto-array.conf`, or
`/etc/slurm-auto-array.conf` if that doesn't exist. If the `SAA_CONFIG` environment variable is specified, the file
specified by it will be used instead.

The format of arguments in the config file is `KEY=value`.

- `SAA_ARG_FILE_DIR`: a directory for storage of ephemeral argument files (default `$HOME/.local/share/saa-arg-files`)
- `SAA_MAX_ARRAY_TASKS`: the maximum number of array tasks for a job array (default 1000)
- `SAA_DEFAULT_WORK_UNIT_SIZE`: the default work unit size (default `1,0,1G,1:00:00`)
- `SAA_DEFAULT_ARRAY_TASK_SIZE`: the default array task size (default `4,1,16G,6:00:00`)
- `SAA_MAX_ARRAY_TASK_SIZE`: the largest possible array task (default `1024,64,16T,30-00:00:00`)

The same keys can also be specified as environment variables, which will supersede the settings in the config file.



## BUGS

`sbatch` flags can be separated from their values and still parse--for instance, the following would work, running the
job array on the `myqos` QOS:

`slurm-auto-arry --qos --verbose myqos -- mycmd :::: my-args.txt`



## AUTHOR

Michael Greenburg (michael_greenburg@byu.edu)



## SEE ALSO

**sbatch(1)**

Worked example at https://rc.byu.edu/wiki/?id=slurm-auto-array
