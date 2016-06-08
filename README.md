## NBA SportVU Tracking Data
##### by Felipe Chamma, Felipe Ferreira, Alex Morris, Ryan Speed
Repository containing code to aggregate NBA location data using distributed computing technologies.

### Dataset
SportVU is a camera system hung from the rafters that collects tracking data at a rate of 25 Hertz (25 times per second), or equivalently every 40 milliseconds. We define a moment as one of the 40 millisecond snapshots. The system follows the ball and every player on the court, recording 11 total observations per moment. For each moment a coordinate (x, y) is recorded for each player, representing his location on the court. Where x ranges from 0 to 94 and y ranges from 0 to 50, the width and length of the court respectively. From this system we were able to acquire data across 90 thousand moments, totaling approximately 1 million observations per game, for 2460 games; The entire 2014-2015 NBA season. The tracking data was very raw, and needed a significant amount of cleaning, processing, and manipulation before we could gain any insights.

### Distributed Environment
Our distributed environment was on AWS EMR and where we set up a cluster containing 6 nodes (1 master, 5 slave). The EC2 instance type of each node was an m3.2xlarge which is a memory optimized instance. We used the same cluster for both Hive and Spark parts of the project. The software we chose to install was Hive 1.0.0, Spark 1.6.1 and Zeppelin-Sandbox 0.5.6. We carried out most of the querying in Zeppelin.

### Problem
Initially, we wanted to create a vector to represent each player’s locations on the basketball court, then cluster the vectors. We were successful using python locally and using Spark, however the task was not suitable for a querying language such as hive. In order to properly test and compare different distribution systems, with our local environment, we shifted our approach. We are going to test an algorithm which implements a euclidean distance measure aggregated by player. Given the data we are trying to calculate total distance ran per player using windowing function queries using Spark, Hive & locally on PostgreSQL. We were able to use all of the same preprocessing and data loading from the clustering problem.
