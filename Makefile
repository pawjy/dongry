## Run tests:
##   $ make test
## Update dependency list:
##   $ make pmb-update
## Install dependent modules into ./local/:
##   $ make pmb-install
## Create tarballs for distribution:
##   $ make dist

all:

PROVE = ./prove
GIT = git

## ------ Setup ------

deps: git-submodules pmbp-install

Makefile-setupenv: Makefile.setupenv
	$(MAKE) --makefile Makefile.setupenv setupenv-update \
            SETUPENV_MIN_REVISION=20120930

Makefile.setupenv:
	wget -O $@ https://raw.github.com/wakaba/perl-setupenv/master/Makefile.setupenv

pmb-update: pmbp-update
pmb-install: pmbp-install
local-perl: pmbp-install
lperl: pmbp-install

pmbp-update pmbp-install generatepm: %: Makefile-setupenv
	$(MAKE) --makefile Makefile.setupenv $@

git-submodules:
	$(GIT) submodule update --init

## ------ Tests ------

test: safetest

test-deps: deps

safetest: test-deps
	$(PROVE) t/sql/*.t t/database/*.t t/type/*.t t/table/*.t t/query/*.t

## ------ Packaging ------

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
