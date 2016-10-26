package hdpcd_spark.getting_started

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Exercise 3: Run a Spark job on YARN
 * https://spark.apache.org/docs/1.1.0/cluster-overview.html
 *
 * This exercise has no unit-tests, however it demonstrates how spark-submit
 * can be used to launch a Spark application on a local cluster or in yarn.
 *
 * Complete the main function below, compile the application to a standalone jar (
 * sbt package) and submit a spark application using the following commands:
 *
 * Local machine:
 * ./spark-submit --class hdpcd_spark.getting_started.ex3 \
 * --master local \
 * hdpcd_spark_2.11-1.0.jar
 *
 * Yarn cluster:
 * ./spark-submit --class hdpcd_spark.getting_started.ex3 \
 * --master yarn-cluster \
 * hdpcd_spark_2.11-1.0.jar
 * 
 * https://github.com/holdenk/spark-testing-base/blob/master/src/main/1.3/scala/com/holdenkarau/spark/testing/YARNCluster.scala
 *
 */
object ex3 {

  def main(args: Array[String]) {

    // 1. Create a new conf object, setting the Appname to ex3
    val conf = new SparkConf().setAppName("ex3")

    // 2. Initalise the ex1 object with the SparkConf from above
    ex1.init(conf)

    // 3. Print out the appname
    println(ex1.getAppName)

    // 4. Stop the SparkContext in ex1
    ex1.stop
  }

}