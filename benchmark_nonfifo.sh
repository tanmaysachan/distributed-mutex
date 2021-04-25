#!/bin/bash

for file in *; do
    if [[ $file == *NON_FIFO* ]]; then
        if [[ $1 == "LOW" ]]; then
            timeout 100s mpiexec -n $2 python $file 0.000001 > output.txt
            echo $file `grep -c "CS PASSED" output.txt`
        elif [[ $1 == "HIGH" ]]; then
            timeout 100s mpiexec -n $2 python $file 0.1 > output.txt
            echo $file `grep -c "CS PASSED" output.txt`
        fi
    elif [[ $file == benchmark* || $file == "tmp.txt" || $file == "output.txt" ]]; then
        continue
    fi
done
