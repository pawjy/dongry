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

safetest:
	$(PROVE) t/sql/*.t t/database/*.t t/type/*.t t/table/*.t t/query/*.t

## License: Public Domain.
