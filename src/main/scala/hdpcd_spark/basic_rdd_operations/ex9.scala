package hdpcd_spark.basic_rdd_operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast
import hdpcd_spark.getting_started

/**
 * Exercise 9: Create and use broadcast variables and accumulators
 * http://spark.apache.org/docs/latest/programming-guide.html#shared-variables
 *
 * Use broadcast variables and accumulators
 */
object ex9 {

  // 1. Given an array or strings, return a broadcast variable of that list
  def broadcastArray(array: Array[String]): Broadcast[Array[String]] = {
    common.sc.broadcast(array)
  }

  // 2. Write a filter that filters only strings in a given broadcasted array, and takes an optional
  //    spark accumulator that counts how many words were filtered out
  //    Careful to add the wordcount to the accumulator
  def filterInArray(array: Broadcast[Array[String]], missedAcc: Option[Accumulator[Long]] = None): ((String, Int)) => Boolean = {
    (t: (String, Int)) =>
      if (array.value.contains(t._1)) {
        true
      } else {
        missedAcc.foreach { _.add(t._2) }
        false
      }
  }

}