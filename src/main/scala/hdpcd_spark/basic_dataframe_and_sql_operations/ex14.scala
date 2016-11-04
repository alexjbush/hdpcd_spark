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
 * Exercise 14: Write a Spark SQL application that reads and writes data from Hive tables
 * http://spark.apache.org/docs/1.6.2/sql-programming-guide.html#programmatically-specifying-the-schema
 *
 * Write a Spark SQL application
 */
object ex14 {

  // 1. Insert author, count(title) into the sqlcontext by registering a temp table and inserting it as a create table as select
  def createTableAs(df: DataFrame, name: String): Unit = {
    df.registerTempTable("tmp_" + name)
    common.sqlc.sql("create table " + name + " as select author as author, count(title) as count from tmp_" + name + " group by author order by count(title) desc ")
  }

  // 2. Write an SQL query get the top author and count
  def findAuthorWithMostBooks(name: String): (String, Long) = {
    val row = common.sqlc.sql("select author, count from " + name + " order by count desc limit 1").head()
    (row.getString(0), row.getLong(1))
  }

}