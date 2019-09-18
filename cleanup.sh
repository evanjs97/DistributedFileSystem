#!/usr/bin/env bash

#!/usr/bin/env bash

HOME="$( cd "$( dirname "$0" )" && pwd )"
CLEAN_DIR="/tmp/evanjs/*"

SCRIPT="rm -r ${CLEAN_DIR} || true;"
sleep 3
for i in `cat machine_list`; do
	echo 'logging into '${i}
	OPTIONS="ssh $i $SCRIPT"
	eval $OPTIONS
done
