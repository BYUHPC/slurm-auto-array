# `slurm-auto-array`

`slurm-auto-array` provides users of [`sbatch`](https://slurm.schedmd.com/sbatch.html) an easier way to submit [job arrays](https://slurm.schedmd.com/job_array.html), especially when their **work units** are very small and/or very numerous. Rather than creating a mapping from [`SLURM_ARRAY_TASK_ID`](https://slurm.schedmd.com/job_array.html#env_vars) to the arguments they want to run a command on, they supply said arguments directly over stdin much like one would with [`parallel`](https://www.gnu.org/software/parallel/). or [`xargs`](https://manpages.org/xargs). For example, to run `mycmd --infile $FILE` on every `FILE` with a name ending in "`.in`" in the directory `infiles`, allowing each work unit 1 GiB of memory and a processor for an hour, one could use:

```shell
find infiles/*.in | slurm-auto-array --time 1:00 --ntasks 1 --mem 1G -- mycmd --infile
```

Although we've found `slurm-auto-array` to work well for many users on our system, it's still a rough draft that hasn't been tested elsewhere--**treat it as early beta software**. [slurm-array-submit](https://github.com/juliangilbey/slurm-array-submit) is another option.



## Installation

`slurm-auto-array` requires [Slurm](https://slurm.schedmd.com/overview.html), [GNU Parallel](https://www.gnu.org/software/parallel/), and Python (3.6 or higher) at runtime. Pandoc is required to install from this repository, and `bats` is required to run `make check`.

Given that you want to install in `/my/software/slurm-auto-array`:

```shell
aclocal
autoconf
automake --add-missing
./configure --prefix=/my/software/slurm-auto-array
make check
make install
```

You'll need to run `parallel --citation; parallel --record-env` in a clean environment before `slurm-auto-array` or `make check` will work.



## Features

### Aggregation

`slurm-auto-array` aggregates work since job arrays consisting of many jobs are hard on the scheduler. If a user wants to run a command on each of 100,000 files, `slurm-auto-array` will by default submit at most 1,000 jobs, each in charge of at least 100 work units (which are run with `parallel`). The parameters that determine the amount of work units that each array task runs can be tuned; see the **configuration** section of the man page. Despite the aggregation, output files can still be made distinct per work unit.

### Batch Script Parsing

`#SBATCH` directives in submission scripts are parsed and passed to `sbatch`, so unmodified batch scripts can be used (almost) seamlessly by `slurm-auto-array`.



## TODO

Documentation and tests are by far the most important improvements needed. Outside of that, it would be nice to have:

- Functionality similar to `parallel`'s "`:::`" to allow users to cross sets of arguments
- Dynamic scheduling with a work database and consumer workers
- Add parsing of `#SAA` arguments in submission scripts
- A release tarball and instructions on `make dist`

In addition to the usual bugs and feature requests, please open an issue if you find any of the documentation confusing--the original purpose of `slurm-auto-array` was to make it easy for users to submit job arrays, and bad documentation defeats that core purpose.
