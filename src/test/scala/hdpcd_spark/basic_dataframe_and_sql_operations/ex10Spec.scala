package hdpcd_spark.basic_dataframe_and_sql_operations

import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Accumulator
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import hdpcd_spark.basic_rdd_operations._

class ex10Spec extends FunSuite with MustMatchers with BeforeAndAfter {

  before {
    // Set up the spark context
    common.init(new SparkConf().setAppName("ex10").setMaster("local[*]"))
  }

  test("broadcast an array") {
    val test_array = Array("ahab", "ishmael", "queequeg", "pip", "moby")
    ex9.broadcastArray(test_array).value must be(test_array)
  }

  test("test df creation from rdd") {
    val test_rdd = ex5.readFile(getClass.getResource("/mobydick/2701-0.txt").getPath)
    val rdd = ex7.getWordcount(test_rdd)
    val rowrdd = ex10.toRowRDD(rdd)
    rowrdd.sortBy(_.getInt(1), false).first().getString(0) must be("the")
  }

  test("check wordcount schema") {
    ex10.wordcountSchema must be(StructType(Seq(
      StructField("word", StringType),
      StructField("count", IntegerType))))
  }

  test("test df creation from json") {
    val test_df = ex10.jsonToDataframe(getClass.getResource("/gb_books.json.gz").getPath)
    test_df.columns must be(Array("LCC", "author", "authoryearofbirth", "authoryearofdeath", "downloads", "formats", "id", "language", "subjects", "title", "type"))

  }

  after {
    common.stop
  }

}