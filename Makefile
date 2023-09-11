DESTDIR ?= /usr/local
MD2MAN ?= go-md2man
MAN = share/man/man1/slurm-auto-array.1

.PHONY: install
install:
	cp -r --preserve=mode bin lib VERSION.txt "$(DESTDIR)"
	mkdir -p "$(DESTDIR)"/share/man/man1/
	cp $(MAN) "$(DESTDIR)"/$(MAN)

.PHONY: man # run this before commit
man:
	$(MD2MAN) < $(MAN).md | sed "s/@VERSION/$(shell cat VERSION.txt)/g; s/@DATE/$(shell date +'%B %Y')/g" > $(MAN)
