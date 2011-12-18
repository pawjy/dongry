PROVE = prove

all:

test: safetest

safetest:
	$(PROVE) t/sql/*.t t/database/*.t t/table/*.t

## License: Public Domain.
