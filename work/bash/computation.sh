#!/bin/bash
start=`date +%s`
num_procs=$1
num_jobs="\j" 

echo "Computation Started!"
i=0
while true; do
  while (( ${num_jobs@P} >= num_procs )); do
    wait -n
    STOP=$(python ./process/get_status.py)
  done
  if ((STOP)); then
    sleep 10
    STOP=$(python ./process/get_status.py)
    #echo "stop $STOP"
    continue
  fi
  ((i++))
  #echo "Process job: ${i}"
  python ./process/computation.py &
done

