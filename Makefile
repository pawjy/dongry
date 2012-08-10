## Run tests:
##   $ make test
## Update dependency list:
##   $ make pmb-update
## Install dependent modules into ./local/:
##   $ make pmb-install
## Create tarballs for distribution:
##   $ make dist

all:

Makefile-setupenv: Makefile.setupenv
	$(MAKE) --makefile Makefile.setupenv setupenv-update \
            SETUPENV_MIN_REVISION=20120318

Makefile.setupenv:
	wget -O $@ https://raw.github.com/wakaba/perl-setupenv/master/Makefile.setupenv

local-perl perl-version perl-exec \
lperl pmb-update pmb-install \
generatepm: %: Makefile-setupenv
	$(MAKE) --makefile Makefile.setupenv $@

PROVE = prove
PERL_VERSION = latest
PERL_PATH = $(abspath local/perlbrew/perls/perl-$(PERL_VERSION)/bin)

test: safetest

test-deps: git-submodules pmb-install

GIT = git

git-submodules:
	$(GIT) submodule update --init

safetest: test-deps
	PATH=$(PERL_PATH):$(PATH) PERL5LIB=$(shell cat config/perl/libs.txt) \
            $(PROVE) t/sql/*.t t/database/*.t t/type/*.t \
	        t/table/*.t t/query/*.t

GENERATEPM = local/generatepm/bin/generate-pm-package
GENERATEPM_ = $(GENERATEPM) --generate-json

dist: generatepm
	mkdir -p dist
	$(GENERATEPM_) config/dist/dongry.pi dist
	$(GENERATEPM_) config/dist/dongry-type-datetime.pi dist
	$(GENERATEPM_) config/dist/dongry-type-json.pi dist
	$(GENERATEPM_) config/dist/dongry-type-messagepack.pi dist

dist-wakaba-packages: local/wakaba-packages dist
	cp dist/*.json local/wakaba-packages/data/perl/
	cp dist/*.tar.gz local/wakaba-packages/perl/
	cd local/wakaba-packages && $(MAKE) all

local/wakaba-packages: always
	git clone "git@github.com:wakaba/packages.git" $@ || (cd $@ && git pull)
	cd $@ && git submodule update --init

always:

## License: Public Domain.
