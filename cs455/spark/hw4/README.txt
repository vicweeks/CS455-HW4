--------------------------
Diego Batres, Victor Weeks, Josiah May
CS455 - Distributed Systems
HW-4: Music Genre Classification using Machine Learning Algorithms
--------------------------



Introduction:
-------------------------------------

The goal of our project is to explore different machine learning algorithms to correctly classify songs by genre, and compare their accuracy and performance using data from the Million Song Dataset. The output will allow us to see how close our results were to the expected values and compare the results with other algorithms. The algorithms we will be exploring are:

- Decision Trees
- Random Forest
- Logistic Regression w/ Elastic Net



Commands:
-------------------------------------

> mvn package              	(make)
> mvn clean			(clean)


To run HW4: 
> $SPARK_HOME/bin/spark-submit --class "HW4" --master yarn target/hw4-project-1.0.jar [input folder]


Breakdown of classes:
------------------------------------

    - util
       - MusicGenreTerms.java: categorizes key terms and most popular subgenres under a broader genre
       - RowParser.java: parses data into usable format
    - FindMostPopularGenre.java: finds total count of first key terms of song
    - FindTheGenre.java: runs genre analysis using extracted data
    - HW4.java: reads in data
    - Song.java: class template for song metadata