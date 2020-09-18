#!/bin/bash
JAR_PATH="./ProgettoHadoop-1.0.jar it.unipi.hadoop.Main"
PROJECT_PATH="/user/progetto"

hadoop fs -rm -skipTrash ${PROJECT_PATH}/centroids/*
hadoop fs -rmdir ${PROJECT_PATH}/centroids

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
            hadoop jar ${JAR_PATH} 2 3 "$(cat ../Dataset/2-3-1000-initial_centroids.txt)"    ${PROJECT_PATH}/2-3-1000-dataset.txt    ${PROJECT_PATH}/centroids
            break
            ;;
        ${benchmarks[1]})
            hadoop jar ${JAR_PATH} 2 3 "$(cat ../Dataset/2-3-10000-initial_centroids.txt)"   ${PROJECT_PATH}/2-3-10000-dataset.txt   ${PROJECT_PATH}/centroids
            break
            ;;
		${benchmarks[2]})
            hadoop jar ${JAR_PATH} 2 3 "$(cat ../Dataset/2-3-100000-initial_centroids.txt)"  ${PROJECT_PATH}/2-3-100000-dataset.txt  ${PROJECT_PATH}/centroids
            break
            ;;
		${benchmarks[3]})
            hadoop jar ${JAR_PATH} 2 3 "$(cat ../Dataset/2-3-1000000-initial_centroids.txt)" ${PROJECT_PATH}/2-3-1000000-dataset.txt ${PROJECT_PATH}/centroids
            break
            ;;
		${benchmarks[4]})
            hadoop jar ${JAR_PATH} 3 3 "$(cat ../Dataset/3-3-1000-initial_centroids.txt)"    ${PROJECT_PATH}/3-3-1000-dataset.txt    ${PROJECT_PATH}/centroids
            break
            ;;
        ${benchmarks[5]})
            hadoop jar ${JAR_PATH} 3 3 "$(cat ../Dataset/3-3-10000-initial_centroids.txt)"   ${PROJECT_PATH}/3-3-10000-dataset.txt   ${PROJECT_PATH}/centroids
            break
            ;;
		${benchmarks[6]})
            hadoop jar ${JAR_PATH} 3 3 "$(cat ../Dataset/3-3-100000-initial_centroids.txt)"  ${PROJECT_PATH}/3-3-100000-dataset.txt  ${PROJECT_PATH}/centroids
            break
            ;;
		${benchmarks[7]})
            hadoop jar ${JAR_PATH} 3 3 "$(cat ../Dataset/3-3-1000000-initial_centroids.txt)" ${PROJECT_PATH}/3-3-1000000-dataset.txt ${PROJECT_PATH}/centroids
            break
            ;;
		${benchmarks[8]})
            hadoop jar ${JAR_PATH} 3 7 "$(cat ../Dataset/3-7-1000-initial_centroids.txt)"    ${PROJECT_PATH}/3-7-1000-dataset.txt    ${PROJECT_PATH}/centroids
            break
            ;;
        ${benchmarks[9]})
            hadoop jar ${JAR_PATH} 3 7 "$(cat ../Dataset/3-7-10000-initial_centroids.txt)"   ${PROJECT_PATH}/3-7-10000-dataset.txt   ${PROJECT_PATH}/centroids
            break
            ;;
		${benchmarks[10]})
            hadoop jar ${JAR_PATH} 3 7 "$(cat ../Dataset/3-7-100000-initial_centroids.txt)"  ${PROJECT_PATH}/3-7-100000-dataset.txt  ${PROJECT_PATH}/centroids
            break
            ;;
		${benchmarks[11]})
            hadoop jar ${JAR_PATH} 3 7 "$(cat ../Dataset/3-7-1000000-initial_centroids.txt)" ${PROJECT_PATH}/3-7-1000000-dataset.txt ${PROJECT_PATH}/centroids
            break
            ;;
		${benchmarks[12]})
            hadoop jar ${JAR_PATH} 3 13 "$(cat ../Dataset/3-13-1000-initial_centroids.txt)"    ${PROJECT_PATH}/3-13-1000-dataset.txt    ${PROJECT_PATH}/centroids
            break
            ;;
        ${benchmarks[13]})
            hadoop jar ${JAR_PATH} 3 13 "$(cat ../Dataset/3-13-10000-initial_centroids.txt)"   ${PROJECT_PATH}/3-13-10000-dataset.txt   ${PROJECT_PATH}/centroids
            break
            ;;
		${benchmarks[14]})
            hadoop jar ${JAR_PATH} 3 13 "$(cat ../Dataset/3-13-100000-initial_centroids.txt)"  ${PROJECT_PATH}/3-13-100000-dataset.txt  ${PROJECT_PATH}/centroids
            break
            ;;
		${benchmarks[15]})
            hadoop jar ${JAR_PATH} 3 13 "$(cat ../Dataset/3-13-1000000-initial_centroids.txt)" ${PROJECT_PATH}/3-13-1000000-dataset.txt ${PROJECT_PATH}/centroids
            break
            ;;
        "Quit")
            echo "User requested exit"
            exit
            ;;
        *) echo "invalid option $REPLY";;
    esac
done
