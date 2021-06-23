# Big-Data-Movie-Reviews
Apache Spark was used to process and analyse large movie datasets i.e. Spark was used to process Big Data.

## Introduction

<p align = "justify">
“Processed data is information. Processed information is knowledge. Processed knowledge is Wisdom.” (Ankala V. Subbarao) Essentially, data is very important. The humungous increase in big data in the past decade has been unprecedented leading to the creation of "Big Data". Efficiently storing, processing and analyzing this data is the crux of "Big Data". The extent of the growth of data has motivated the creation of Big Data tools like Hadoop MapReduce and Apache Spark. These tools can be used to successfully store and process huge amounts of data. Hadoop MapReduce causes a lot of disk I/O which is both expensive and can cause delays which leads to the main advantage of Apache Spark. Spark stores intermediate data in memory thereby reducing disk I/O and improving performance. Also, Spark can be used for near-real time analysis. </p>

<p align = "justify">
In this project, the main objective was to work with two datasets: movies and reviews and use Spark to perform operations on them. The first task was to rank the movies by their popularity i.e. the number of reviews each movie had and then select the top ten highest ranked movies. The second task was to find all the movies which had an average rating greater than 4 stars and more than 10 reviews. Each dataset had two sizes: the small size had 100, 000 rows while the large size had 26 million rows. Therefore, there were four text files we worked with. </p>

## Aim

Task 1: Rank movies by their popularity <br>
Task 2: Find all the movies which had more than 10 user reviews and an average rating of greater than 4 stars

<img width="572" alt="Screen Shot 2021-06-23 at 8 18 48 PM" src="https://user-images.githubusercontent.com/32781544/123133016-2de6f300-d404-11eb-8dd1-51c5c572c0d7.png">

<img width="555" alt="Screen Shot 2021-06-23 at 8 18 47 PM" src="https://user-images.githubusercontent.com/32781544/123132967-24f62180-d404-11eb-84c8-a4afa33d8d59.png">
