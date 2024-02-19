# Twitter Big Data Analysis

This project focuses on analyzing Twitter follower relationships to identify patterns such as follower counts and social triangles (where user A follows B, B follows C, and C follows A) using Apache Spark and MapReduce. The analysis aims to understand the distribution and dynamics of Twitter social networks through scalable big data processing techniques.

## Project Overview

We implemented various programs in Spark and MapReduce to perform complex data aggregation, joins, and analysis tasks. Our primary objectives were to count the number of followers for selected users and to identify social triangles within the Twitter dataset.

## Features

- **Follower Count Analysis**: Implementations in Spark using different RDD operations (groupByKey, reduceByKey, foldByKey, aggregateByKey) and DataSet/DataFrame aggregations to count followers.
- **Social Triangle Detection**: Developed MapReduce and Spark applications to detect social triangles, employing techniques such as reduce-side joins and replicated joins.

## Technologies Used

- Apache Spark for scalable data processing and analysis.
- Hadoop MapReduce for implementing joins and aggregation tasks on large datasets.
- AWS Elastic MapReduce (EMR) for executing programs in a distributed environment.

## Getting Started

### Prerequisites

- Apache Spark (we used version 2.4.5)
- Hadoop (tested on version 2.7.3)
- AWS CLI, configured for access to AWS EMR clusters
- Scala (for Spark implementations) or Java (for MapReduce implementations)

### Installation and Running

1. Clone this repository to your local machine or cloud environment.

2. Ensure Spark and/or Hadoop are installed and properly configured on your system.

3. For Spark programs:
   - Navigate to the Spark program directory.
   - Use `spark-submit` to run the follower count or triangle detection programs. Example:
     ```
     spark-submit --class org.example.TwitterFollowerCount target/twitter-analysis-1.0-SNAPSHOT.jar
     ```

4. For MapReduce programs:
   - Compile the Java program using Maven or a similar tool to produce a JAR file.
   - Use `hadoop jar` to submit the job to a Hadoop cluster. Example:
     ```
     hadoop jar twitter-analysis-mapreduce-1.0-SNAPSHOT.jar org.example.TriangleCount
     ```

5. To execute programs on AWS EMR:
   - Use the AWS CLI or AWS Management Console to create an EMR cluster.
   - Submit Spark or MapReduce jobs using the `add-steps` command or through the EMR console.

## Documentation

For detailed information on the implementation and execution of each program, refer to the `docs` folder.

## Contributing

Contributions to improve the project are welcome. Please follow the standard fork-pull request workflow.

## License

This project is licensed under the MIT License - see the LICENSE.md file for details.
