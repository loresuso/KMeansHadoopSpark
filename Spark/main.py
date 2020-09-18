import sys
import time
from pyspark import SparkContext

centroids_broadcast = 0
THRESHOLD = 0.1
CURRENT_PRINTING_LEVEL = 0


def divide_coords(point):
    """
        point[0] -> the old centroid
        point[1] -> the new centroid (still with its coords that have to be divided)
        
        A centroid is a tuple of this kind:
        ([coord0, coord1, coord2, ....], weight)
    """
    centroid = point[1]
    weight = centroid[1]
    coords = centroid[0]
    coords = [coord / float(weight) for coord in coords]
    return coords, None


def kmeans_reduce(point_a, point_b):
    """
        Example:
        point_a: ([13.44355837000283, 15.595223195294551], 1)
        point_b: ([12.279225084444104, 16.040184097277034], 1)
    """

    total_weight = point_a[1] + point_b[1]
    point_sum = []
    dim = len(point_a[0])
    for i in range(dim):
        point_sum.append(float(point_a[0][i]) + float(point_b[0][i]))

    return point_sum, total_weight


def euclidean_distance(a, b):
    sum = 0
    for i in range(len(a)):
        diff = a[i] - b[i]
        sum += diff * diff
    return sum


def kmeans_map(point):
    point = from_str_to_point(point)
    centroids = centroids_broadcast.value
    min_distance = 10000000
    min_index = -1
    for i in range(len(centroids)):
        distance = euclidean_distance(centroids[i], point)
        if min_distance > distance:
            min_distance = distance
            min_index = i
    return min_index, (point, 1)


def converge(array_centroids_old, array_centroids_new):
    centroid_number = len(array_centroids_old)
    if centroid_number != len(array_centroids_new):
        return false
    for i in range(centroid_number):
        if euclidean_distance(array_centroids_old[i], array_centroids_new[i]) > THRESHOLD:
            return False
    return True



def main():
    global centroids_broadcast
    
    centroids_string = "13.26573242704478,18.07303339406258;11.965183977426816,17.908125592420962;13.322344279783637,16.76039819260074;13.44355837000283,15.595223195294551"
    points_file = "2-4-10000-hadoop.txt"
    
    argc = len(sys.argv)
    
    if argc > 1:
        centroids_string = sys.argv[1]
    
    if argc > 2:
        points_file = sys.argv[2]

    master = "local[*]"
    sc = SparkContext(master, "K-means")
    
    centroids = from_str_to_centr(centroids_string)
    
    start_time = time.time()
    
    centroids.sort()
    centroids_broadcast = sc.broadcast(centroids)
    
    points_rdd = sc.textFile(points_file).cache()
    
    interation_count = 0
    
    while True:
        interation_count += 1
        print("Iteration count: " + str(interation_count))
    
        old_cetroids = centroids_broadcast.value
        
        print("=== Centroidi in ingresso all'iterazione {} ===".format(interation_count))
        for old_centroid in old_cetroids:
            print("  Centroide: " + str(old_centroid))
        
        points_rdd_map = points_rdd.map(lambda x: kmeans_map(x))
        points_rdd_reduce = points_rdd_map.reduceByKey(lambda x, y: kmeans_reduce(x, y))
        new_centroids_rdd = points_rdd_reduce.map(lambda x: divide_coords(x))
        new_centroids = new_centroids_rdd.collect()
        for i in range(len(new_centroids)):
            new_centroids[i] = new_centroids[i][0]
        new_centroids.sort()
        
        print("=== Centroidi in uscita all'iterazione {} ===".format(interation_count))
        for new_centroid in new_centroids:
            print("  Centroide: " + str(new_centroid))
        
        if converge(old_cetroids, new_centroids):
            break
        
        centroids_broadcast = sc.broadcast(new_centroids)
    
    end_time = time.time()
    
    print("Completed after {} seconds".format(end_time-start_time))


def from_str_to_point(points_string):
    point_string = points_string.replace('\n', '')
    point = point_string.split(',')
    for j in range(len(point)):
        point[j] = float(point[j])

    return point


def from_str_to_centr(centroids_string):
    centroids_string = centroids_string.split(';')
    centroids = []
    for i in range(len(centroids_string)):
        point = centroids_string[i].split(',')
        for j in range(len(point)):
            point[j] = float(point[j])
        centroids.append(point)

    return centroids


if __name__ == '__main__':
    main()
