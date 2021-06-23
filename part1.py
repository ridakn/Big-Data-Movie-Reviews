import pyspark
from pyspark.sql import SparkSession

# Task 1: Rank the movie by their number of reviews(popularity) and select the top 10 highest ranked movies.

def task1(reviews, movies):
        
    # group the reviews by the movie ID
    reviews2 = reviews.groupBy(lambda line: line[1]).mapValues(list)
    
    # Get top 10 movies with highest number of reviews 
    top10 = reviews2.takeOrdered(10, key = lambda line: -len(line[1]))  
    
    # Use movie IDs to get movie names of top 10 highest ranked films
    final = []
    for y in top10:
        final.append([movies.filter(lambda x : x[0] == y[0]).collect()[0][1], "\t", len(y[1])])

    return final


if __name__ == '__main__':
    
    # Start spark context
    sc = pyspark.SparkContext('local[*]')
    
    # Start a spark session using spark context
    sess = SparkSession(sc)

    # Read in reviews and movies as CSV then convert to RDDs
    
    reviews = sess.read.csv('/Users/ridakhan/Desktop/reviews.csv', header=True).rdd
    movies = sess.read.csv('/Users/ridakhan/Desktop/movies.csv', header=True).rdd

    # Call task 1 with both files
    final = task1(reviews, movies)
    
    # Save the output
    
    f = open("Task1.txt", 'w')
    f.write("Movie Name \t Number of Reviews \n")
    
    for y in final:
        s = str(y[0]) + str(y[1]) + str(y[2]) + "\n"
        f.write(s)
    