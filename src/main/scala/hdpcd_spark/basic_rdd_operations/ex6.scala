package hdpcd_spark.basic_rdd_operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import hdpcd_spark.getting_started

/**
 * Exercise 6: Persist an RDD in memory or on disk
 * http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence
 *
 * Persist an RDD to Memory
 */
object ex6 {

  // 1. Given an RDD, persist it to memory only
  def persistToMemory[A](rdd: RDD[A]): RDD[A] = {
    rdd.persist()
  }

}