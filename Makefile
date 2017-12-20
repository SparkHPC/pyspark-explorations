# For this to work in your environment, you'll have to specify these variables
# on the command line to override.

SHELL=/bin/bash

all:
	echo "Nothing to do"

cobalt_clean:
	rm -f *.error *.cobaltlog *.output

clean:
	make cobalt_clean
