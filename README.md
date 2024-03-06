# `slurm-auto-array`

`slurm-auto-array` provides users of [`sbatch`](https://slurm.schedmd.com/sbatch.html) an easier way to submit [job arrays](https://slurm.schedmd.com/job_array.html), especially when their **work units** are very small and/or very numerous. Rather than manually creating a mapping from [`SLURM_ARRAY_TASK_ID`](https://slurm.schedmd.com/job_array.html#env_vars) to the arguments they want to run a command on, they supply said arguments directly over stdin much like one would with [`parallel`](https://www.gnu.org/software/parallel/) or [`xargs`](https://manpages.org/xargs). For example, to run `mycmd --infile $FILE` on every `FILE` with a name ending in "`.in`" in the directory `infiles`, allowing each work unit 1 GiB of memory and a processor for an hour, one could use:

```shell
ls infiles/*.in | slurm-auto-array --time 1:00:00 --ntasks 1 --mem 1G -- mycmd --infile
```

...or, equivalently:

```shell
slurm-auto-array --time 1:00:00 --ntasks 1 --mem 1G -- mycmd --infile :::: <(ls infiles/*.in)
```

`slurm-auto-array` aggregates work since job arrays consisting of many jobs are hard on the scheduler. If a user wants to run a command on each of 100,000 files, `slurm-auto-array` will by default submit at most 1,000 jobs, each in charge of at least 100 work units (which are run with `parallel`). The parameters that determine the amount of work units that each array task runs can be tuned; see [the **configuration** section of the man page](share/man/man1/slurm-auto-array.1.md#configuration). Despite the aggregation, output files can still be made distinct per work unit.

Although we've found `slurm-auto-array` to work well [for many users on our system](https://rc.byu.edu/), it's still a rough draft that hasn't been tested elsewhere--**treat it as early beta software**. [slurm-array-submit](https://github.com/juliangilbey/slurm-array-submit) is another option.



## Installation

`slurm-auto-array` requires [Slurm](https://slurm.schedmd.com/overview.html), [GNU Parallel](https://www.gnu.org/software/parallel/), and Python (3.6 or higher) at runtime. Pandoc is required to install from this repository (but *not* to install from a [release](https://github.com/BYUHPC/slurm-auto-array/releases)), and `bats` and all the runtime dependencies are required to run `make check`.

Given that you want to install in `/my/software/slurm-auto-array`:

```shell
# Only needed if installing from git directly:
aclocal
autoconf
automake --add-missing
# Needed for any installation method:
./configure --prefix=/my/software/slurm-auto-array
make check
make install
```

You'll need to run `parallel --citation; parallel --record-env` in a clean environment before `slurm-auto-array` or `make check` will work.

`make dist` will create a release tarball.



## Usage

In its simplest form, `slurm-auto-array` runs a command on on each of several user-supplied arguments--for example, to run `echo 1`, `echo 2`, and `echo 3`, you could submit with any of:

```shell
slurm-auto-array --output echo-%1.txt -- echo ::: 1 2 3
slurm-auto-array --output echo-%1.txt -- echo :::: <(seq 3)
seq 3 | slurm-auto-array --output echo -%1.txt -- echo
```

This will result in 3 files, `echo-1.txt`, `echo-2.txt`, and `echo-3.txt`, each containing the number in its title.

If no `:::`, `:::+`, `::::`, or `::::+` arguments are specified, arguments are taken from stdin; if colon arguments are given, stdin is passed to the command to be run. One could thus use the following to get outputs `1 a b c`, `2 a b c`, and `3 a b c` in the files `slurm-auto-array-*.out`:

```shell
echo a b c | slurm-auto-array -- bash -c 'echo "$0 $(cat)"' :::: <(seq 3)
```

Multiple sets of arguments can be specified with `:::`, in which case the arguments will be crossed. To run `mycommand $letter $number` for every combination of `letter` between `A` and `D` and every `number` between 4 and 10, allocating 2 CPUs and 4 GB of memory for 3 hours for each run, use:

```shell
slurm-auto-array -n 2 --mem 4G -t 3:00:00 -- mycommand ::: A B C D ::: 4 5 6 7 8 9 10
```

Arguments can be paired rather than being crossed by using `:::+` rather than `:::`. To run `echo 1 X a`, `echo 2 Y b`, and `echo 3 Z c`, use:

```shell
slurm-auto-array -- echo ::: 1 2 3 :::+ X Y Z :::+ a b c
```

Use 4 colons rather than 3 to specify a file containing arguments rather than the arguments themselves. To run `echo $N alpha a` and `echo $N beta b` for each `N` from 3 to 8, run:

```shell
slurm-auto-array -- echo :::: <(seq 3 8) ::: alpha beta ::::+ latin_letters.txt
```



## Worked example

Suppose you have many input files scattered about the deep directory `infiles`, each named `*blah*.in`. For each of these files, you'd like to run the equivalent of:

```bash
mycommand --permutation $N \
          --infile "$INFILE" \
          --outfile "${INFILE%.in}-$N.out" \
          &> "${INFILE%.in}-$N.log"
```

...for each `N` in 1 through 4, creating a `*blah*-$N.out` for each input file with `mycommand` and capturing the output in `*blah*-$N.log`. For convenience, you create a script, `run-mycommand.sh`, that takes two arguments: the permutation, and the filename stripped of its suffix. Here it is:

```bash
#!/bin/bash

N="$1"
IN="$2.in"
OUT="$2-$N.out"

mycommand --permutation "$N" --infile "$IN" --outfile "$OUT"
```

To use this with `slurm-auto-array`, you'll need the input files stripped of their suffix:

```bash
find infiles -name '*blah*.in' | sed 's/\.in$//'
```

...and the permutations, which can be obtained with `seq 4`.

To run each instance of `mycommand` with 8 CPUs and 4 GB of memory for 2 hours using `slurm-auto-array`, you can use:

```bash
slurm-auto-array -n 8 --mem 4g -t 2:00:00 -o %2-%1.log -- \
                 run-mycommand.sh :::: <(seq 4) \
                                  :::: <(find infiles -name '*blah*.in' | sed 's/\.in$//')
```