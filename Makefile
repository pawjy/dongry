PROVE = prove

all:

test: safetest

safetest:
	$(PROVE) t/database/*.t t/table/*.t

## License: Public Domain.
