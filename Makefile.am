AUTOMAKE_OPTIONS = foreign

# Scripts
SAAPY = $(srcdir)/libexec/slurm_auto_array.py
TASK = libexec/task.sh
WORK_UNIT = libexec/work_unit.py
bin_SCRIPTS = $(srcdir)/bin/slurm-auto-array
# libexec_SCRIPTS = $(SAAPY) $(srcdir)/libexec/task.sh $(srcdir)/libexec/work_unit.py
libexec_SCRIPTS = $(SAAPY) $(TASK) $(WORK_UNIT)
$(SAAPY): $(SAAPY).in
	sed "s/SAA_VERSION_STRING/$(VERSION)/g" $< > $@

# Man page
MAN = $(srcdir)/share/man/man1/slurm-auto-array.1
man1_MANS = $(MAN)
$(MAN): $(MAN).md
	pandoc -s $< -t man -o $@ -V footer="grade @VERSION@" -V date="@DATE@"

# Testing
TESTS = test/test.bats

CLEANFILES = $(MAN) $(SAAPY)

EXTRA_DIST = $(bin_SCRIPTS) $(SAAPY).in $(TASK) $(WORK_UNIT) $(TESTS) $(MAN) $(MAN).md $(srcdir)/README.md $(srcdir)/LICENSE
