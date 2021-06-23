part1.py: Task 1 for small dataset
part1b.py: Task 2 for small dataset

part1_large.py: Task 1 for large dataset
part1b_large.py: Task 2 for large dataset

part1_P.py: Task 1 with partitions for small dataset
part1b_P.py: Task 2 with partitions for small dataset

Commands I use to run them: 
For maximum threads: 
/usr/local/Cellar/apache-spark/2.4.5/bin/spark-submit \
--master local[*] part1.py

For 2 threads: 
/usr/local/Cellar/apache-spark/2.4.5/bin/spark-submit \
--master local[2] part1.py

For 8 threads: 
/usr/local/Cellar/apache-spark/2.4.5/bin/spark-submit \
--master local[8] part1.py