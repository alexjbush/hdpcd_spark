package hdpcd_spark.getting_started

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Exercise 2: Initialize a Spark application
 * http://spark.apache.org/docs/latest/programming-guide.html#initializing-spark
 *
 * Create a new local spark conf object and initalise spark using the functions from Ex1
 */
object ex2 {

  // 1. Complete the val definition to create a new sparkconf object with appname ex2
  val conf = new SparkConf().setAppName("ex2").setMaster("local[*]")

  // 2. Complete the function to return the current spark context running status
  def initaliseSpark(): Unit = ex1.init(conf)

}