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
 * Exercise 12: Perform operations on a DataFrame
 * http://spark.apache.org/docs/1.6.2/sql-programming-guide.html#programmatically-specifying-the-schema
 *
 * Write a Spark SQL application
 */
object ex12 {

  // 1. Register the dataframe as a temporary table
  def registerTempTable(df: DataFrame, name: String): Unit = {
    df.registerTempTable(name)
  }

  // 2. Write an SQL query to do the same operation as in ex 11
  def findAuthorWithMostBooks(tableName: String): (String, Long) = {
    val row = common.sqlc.sql("select author, count(title) from " + tableName + " group by author order by count(title) desc limit 1").head()
    (row.getString(0), row.getLong(1))
  }

}