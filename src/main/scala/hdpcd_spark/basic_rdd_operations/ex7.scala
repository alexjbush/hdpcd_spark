package hdpcd_spark.basic_rdd_operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import hdpcd_spark.getting_started

/**
 * Exercise 7: Perform Spark transformations on an RDD
 * http://spark.apache.org/docs/latest/programming-guide.html#rdd-operations
 *
 * Create an RDD of each word and its wordcount
 */
object ex7 {

  // 1. Given a string, return all the words in that string in lower case
  //    The following regex pattern will work with findAllin: [a-zA-Z]+(-[a-zA-Z]+)?
  //    https://www.safaribooksonline.com/library/view/scala-cookbook/9781449340292/ch01s07.html
  def wordsInString(line: String): Array[String] = {
    val pattern = "[a-zA-Z]+(-[a-zA-Z]+)?".r
    pattern.findAllIn(line.toLowerCase()).toArray
  }

  // 2. Use flatmap with our function above, map and reduceByKey to get a list of
  //    each word and its count in the text
  def getWordcount(rdd: RDD[String]): RDD[(String, Int)] = {
    rdd.flatMap { wordsInString(_) }.map { (_, 1) }.reduceByKey(_ + _)
  }

}