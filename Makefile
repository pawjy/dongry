PROVE = prove
GENERATEPMPACKAGE = generate-pm-package

all:

dist: always
	mkdir -p dist
	$(GENERATEPMPACKAGE) config/dist/dongry.pi dist
	$(GENERATEPMPACKAGE) config/dist/dongry-type-datetime.pi dist
	$(GENERATEPMPACKAGE) config/dist/dongry-type-json.pi dist

always:

test: safetest

safetest: local-submodules carton-install config/perl/libs.txt safetest-main

safetest-main:
	PERL5LIB=$(shell cat config/perl/libs.txt) $(PROVE) \
	    t/sql/*.t t/database/*.t t/type/*.t t/table/*.t t/query/*.t

Makefile.setupenv:
	wget -O $@ https://raw.github.com/gist/1883312/Makefile.setupenv

setupenv remotedev-test remotedev-reset config/perl/libs.txt \
carton-install local-submodules: %: Makefile.setupenv always
	make --makefile Makefile.setupenv $@ REMOTEDEV_HOST=$(REMOTEDEV_HOST)

always:

## License: Public Domain.
