#!/usr/bin/env bash

HOME="$( cd "$( dirname "$0" )" && pwd )"

SERVER="juneau"
PORT="45467"
CLIENT="little-rock"
CHUNK_SERVERS_PER_MACHINE=1
CHUNK_DIR="/tmp/evanjs"
#FAULT_TOLERANCE="erasure"

gnome-terminal --geometry=132x43 -e "ssh -t ${SERVER} 'cd ${HOME}/build/classes/java/main; java cs555.dfs.server.ControllerServer ${PORT};bash;'"
gnome-terminal --geometry=132x43 -e "ssh -t ${CLIENT} 'cd ${HOME}/build/classes/java/main; java cs555.dfs.server.ClientServer ${SERVER} ${PORT} ${FAULT_TOLERANCE};bash;'"


SCRIPT="cd ${HOME}/build/classes/java/main; java cs555.dfs.server.ChunkServer ${SERVER} ${PORT} ${CHUNK_DIR};"
COMMAND="gnome-terminal"
sleep 3
for i in `cat machine_list`; do
	for j in `seq 1 ${CHUNK_SERVERS_PER_MACHINE}`; do
      		echo 'logging into '${i}
       		OPTIONS='--tab -e "ssh -t '$i' '$SCRIPT'"'
        	COMMAND+=" $OPTIONS"
    done
done

eval $COMMAND &
