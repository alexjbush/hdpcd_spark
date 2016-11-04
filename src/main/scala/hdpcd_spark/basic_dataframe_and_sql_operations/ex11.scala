package hdpcd_spark.basic_dataframe_and_sql_operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast
import hdpcd_spark.basic_rdd_operations._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }

import org.apache.spark.sql._

/**
 * Exercise 11: Perform operations on a DataFrame
 * http://spark.apache.org/docs/1.6.2/sql-programming-guide.html#dataframe-operations
 *
 * Use a dataframe to get the top author
 */
object ex11 {

  // 1. Given the books dataframe from ex10, return the author with the most publications
  //    and the publication count as a tuple
  def getAuthorWithMostBooks(df: DataFrame): (String, Long) = {
    val sqlc = common.sqlc
    import sqlc.implicits._
    val row = df.groupBy("author").count().sort($"count".desc).head()
    (row.getString(0), row.getLong(1))
  }

}