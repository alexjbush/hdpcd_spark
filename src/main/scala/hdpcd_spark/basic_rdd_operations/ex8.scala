package hdpcd_spark.basic_rdd_operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import hdpcd_spark.getting_started

/**
 * Exercise 8: Perform Spark actions on an RDD
 * http://spark.apache.org/docs/latest/programming-guide.html#actions
 *
 * Return the top n most common words that match a filter
 */
object ex8 {

  // 1. Use filter and sortby to get the top n words that match the filter
  def getTopWords(rdd: RDD[(String, Int)], filter: ((String, Int)) => Boolean, hits: Int = 5): Array[String] = {
    rdd.filter(filter).sortBy(_._2, false).take(hits).map(_._1)
  }

}