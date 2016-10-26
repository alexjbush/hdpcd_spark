package hdpcd_spark.basic_rdd_operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import hdpcd_spark.getting_started

/**
 * Exercise 5: Create an RDD from a file or directory in HDFS
 * http://spark.apache.org/docs/latest/programming-guide.html#external-datasets
 *
 * Create an RRD from a text file in HDFS and count the lines
 */
object ex5 {

  // 1. Create a textFile RDD from a file in HDFS
  def readFile(filename: String): RDD[String] = {
    common.sc.textFile(filename)
  }

  // 2. Call the previous function to get a textFile RDD and return the count of the lines
  def countLines(filename: String): Long = readFile(filename).count()

}