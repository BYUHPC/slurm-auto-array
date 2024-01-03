AUTOMAKE_OPTIONS = foreign

# Scripts
SAAPY = libexec/slurm_auto_array.py
bin_SCRIPTS = bin/slurm-auto-array
libexec_SCRIPTS = $(SAAPY) libexec/task.sh libexec/work_unit.py
$(SAAPY): $(SAAPY).in
	sed "s/SAA_VERSION_STRING/$(VERSION)/g" $< > $@

# Man page
MAN = share/man/man1/slurm-auto-array.1
man1_MANS = $(MAN)
$(MAN): $(MAN).md
	pandoc -s $< -t man -o $@ -V footer="grade @VERSION@" -V date="@DATE@"

# Testing
TESTS = test/test.bats

CLEANFILES = $(MAN) $(SAAPY)

EXTRA_DIST = $(bin_SCRIPTS) $(libexec_SCRIPTS) $(MAN) $(MAN).md README.md LICENSE
