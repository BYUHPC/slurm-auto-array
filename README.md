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

Although we've found `slurm-auto-array` to work well [for many users on our system](https://rc.byu.edu/wiki/?id=slurm-auto-array), it's still a rough draft that hasn't been tested elsewhere--**treat it as early beta software**. [slurm-array-submit](https://github.com/juliangilbey/slurm-array-submit) is another option.



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
