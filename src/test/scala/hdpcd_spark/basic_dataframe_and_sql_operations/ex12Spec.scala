package hdpcd_spark.basic_dataframe_and_sql_operations

import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Accumulator
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import hdpcd_spark.basic_rdd_operations._

class ex12Spec extends FunSuite with MustMatchers with BeforeAndAfter {

  before {
    // Set up the spark context
    common.init(new SparkConf().setAppName("ex9").setMaster("local[*]"))
  }


  test("test df group") {
    val test_df = ex10.jsonToDataframe(getClass.getResource("/gb_books.json.gz").getPath)
    ex12.registerTempTable(test_df, "books")
    ex12.findAuthorWithMostBooks("books") must be(("Various",3228))
  }

  after {
    common.stop
  }

}