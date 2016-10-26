package hdpcd_spark.basic_rdd_operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import hdpcd_spark.getting_started

/**
 * Exercise 4: Create an RDD
 * http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds
 *
 * Create a new local spark conf object and initalise spark using the functions from Ex1
 */
object ex4 {
  
  // Scala array type
  val data = Array(4, 8, 15, 16, 23, 42)
  
  // 1. Convert the scala array into an rdd object
  lazy val rddData = common.sc.parallelize(data)
  
  // 2. Sum the values in the RDD to a single integer
  def sumData(): Int = rddData.sum.toInt
  
}