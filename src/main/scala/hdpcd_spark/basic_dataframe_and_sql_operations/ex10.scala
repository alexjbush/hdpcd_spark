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
 * Exercise 9: Create and use broadcast variables and accumulators
 * http://spark.apache.org/docs/latest/programming-guide.html#shared-variables
 *
 * Use broadcast variables and accumulators
 */
object ex10 {

  // 1. Given the wordcount rdd from the previous section, covert this into a
  //    RDD[Row] object
  def toRowRDD(rdd: RDD[(String, Int)]): RDD[Row] = rdd.map(Row.fromTuple(_))

  // 2. Create a schema with word: String, count: Int data types
  val wordcountSchema = StructType(Seq(
    StructField("word", StringType),
    StructField("count", IntegerType)))

  // 3. Given the RDD[Row] from above, transform this to a dataframe
  def rddToDataframe(rdd: RDD[Row], schema: StructType): DataFrame = {
    common.sqlc.createDataFrame(rdd, schema)
  }

  // 4. Given a JSON file, create a dataframe
  def jsonToDataframe(file: String): DataFrame = {
    common.sqlc.read.json(file)
  }

}