import pyspark
from pyspark.sql import SparkSession

# Task 2: Find all the movies with average rating greater than 4 stars and more than 10 reviews

def task2(reviews, movies):
    
    # Map each record as (Movie ID -> Rating, 1)
    reviews1 = reviews.map(lambda x : [x[1], (x[2], 1)])
    
    # Reduce by the keys which are rating and counts which are summed 
    reviews2 = reviews1.reduceByKey(lambda x, y: (float(x[0]) + float(y[0]), x[1] + y[1]))
    
    # Map and calculate average rating for each record by Sum of ratings / Number of Reviews
    reviews3 = reviews2.map(lambda x : [x[0], float(x[1][0])/float(x[1][1]), x[1][1]])
    
    # Filter the movies that have average rating > 4 and more than 10 reviews 
    reviews4 = reviews3.filter(lambda x : x[1] > 4 and x[2] > 10)
    
    # Use Movie IDs to get names of all movies with average rating greater than 4 stars and more than 10 reviews
    final = []
    for y in reviews4.collect(): 
        final.append([movies.filter(lambda x : x[0] == y[0]).collect()[0][1], "\t",y[1], "\t", y[2]])
        
    return final

if __name__ == '__main__':
    
    # Start spark context
    sc = pyspark.SparkContext('local[*]')
    
    # Start a spark session using spark context
    sess = SparkSession(sc)

    # Read in reviews and movies as CSV then convert to RDDs
     
    reviews = sess.read.csv('/Users/ridakhan/Desktop/reviews_large.csv', header=True).rdd
    movies = sess.read.csv('/Users/ridakhan/Desktop/movies_large.csv', header=True).rdd

    # Call task 2 with both files
    
    final = task2(reviews, movies)
    
    # Save the output
    
    f = open("Task2_Large.txt", 'w')
    f.write("Movie Name \t Average Rating \t Number of Reviews \n")
    
    for y in final:
        s = str(y[0]) + str(y[1]) + str(y[2]) + str(y[3]) + str(y[4]) + "\n"
        f.write(s)
