package hdpcd_spark.getting_started

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Exercise 1: Write a Spark Core application in Scala
 * http://spark.apache.org/docs/latest/programming-guide.html
 *
 * Initialise a Spark Context from an existing conf, stop the context,  and return the running status
 */
object ex1 {

  // Singleton SparkContext object, initially null
  var sc: SparkContext = _

  // 1. Complete the function to create a new spark context from the conf object
  def init(conf: SparkConf): Unit = sc = new SparkContext(conf)

  // 2. Complete the function to return the current spark context running status
  def isRunning(): Boolean = !sc.isStopped

  // 3. Complete the function to stop the running spark context
  def stop(): Unit = sc.stop()
  
  // 4. Complete the function to return the context name
  def getAppName: String = sc.appName

}