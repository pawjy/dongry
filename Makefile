PROVE = prove

all:

test: safetest

safetest:
	$(PROVE) t/database/*.t

## License: Public Domain.
