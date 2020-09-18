#!/bin/bash

SCRIPT_PATH="./main.py"
PROGETTO_PATH="/user/progetto"

PS3='Choose benchmark: '
benchmarks=(
	"   1000 points, dim=2, centroids=3"
	"  10000 points, dim=2, centroids=3"
	" 100000 points, dim=2, centroids=3"
	"1000000 points, dim=2, centroids=3"

	"   1000 points, dim=3, centroids=3"
	"  10000 points, dim=3, centroids=3"
	" 100000 points, dim=3, centroids=3"
	"1000000 points, dim=3, centroids=3"

	"   1000 points, dim=3, centroids=7"
	"  10000 points, dim=3, centroids=7"
	" 100000 points, dim=3, centroids=7"
	"1000000 points, dim=3, centroids=7"

	"   1000 points, dim=3, centroids=13"
	"  10000 points, dim=3, centroids=13"
	" 100000 points, dim=3, centroids=13"
	"1000000 points, dim=3, centroids=13"

	"Quit"
)
select fav in "${benchmarks[@]}"; do
    case $fav in
        ${benchmarks[0]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/2-3-1000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/2-3-1000-dataset.txt"
            break
            ;;
        ${benchmarks[1]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/2-3-10000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/2-3-10000-dataset.txt"
            break
            ;;
		${benchmarks[2]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/2-3-100000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/2-3-100000-dataset.txt"
            break
            ;;
		${benchmarks[3]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/2-3-1000000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/2-3-1000000-dataset.txt"
            break
            ;;
		${benchmarks[4]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/3-3-1000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/3-3-1000-dataset.txt"
            break
            ;;
        ${benchmarks[5]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/3-3-10000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/3-3-10000-dataset.txt"
            break
            ;;
		${benchmarks[6]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/3-3-100000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/3-3-100000-dataset.txt"
            break
            ;;
		${benchmarks[7]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/3-3-1000000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/3-3-1000000-dataset.txt"
            break
            ;;
		${benchmarks[8]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/3-7-1000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/3-7-1000-dataset.txt"
            break
            ;;
        ${benchmarks[9]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/3-7-10000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/3-7-10000-dataset.txt"
            break
            ;;
		${benchmarks[10]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/3-7-100000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/3-7-100000-dataset.txt"
            break
            ;;
		${benchmarks[11]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/3-7-1000000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/3-7-1000000-dataset.txt"
            break
            ;;
		${benchmarks[12]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/3-13-1000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/3-13-1000-dataset.txt"
            break
            ;;
        ${benchmarks[13]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/3-13-10000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/3-13-10000-dataset.txt"
            break
            ;;
		${benchmarks[14]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/3-13-100000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/3-13-100000-dataset.txt"
            break
            ;;
		${benchmarks[15]})
            spark-submit ${SCRIPT_PATH} "$(cat ../Dataset/3-13-1000000-initial_centroids.txt)" "hdfs://172.16.2.36:9000${PROGETTO_PATH}/3-13-1000000-dataset.txt"
            break
            ;;
        "Quit")
            echo "User requested exit"
            exit
            ;;
        *) echo "invalid option $REPLY";;
    esac
done
