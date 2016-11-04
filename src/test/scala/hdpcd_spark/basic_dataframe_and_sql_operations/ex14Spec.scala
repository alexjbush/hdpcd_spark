package hdpcd_spark.basic_dataframe_and_sql_operations

import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Accumulator
import org.apache.spark.sql.hive
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import hdpcd_spark.basic_rdd_operations._
import org.apache.hadoop.test.PathUtils

import java.io.File

class ex14Spec extends FunSuite with MustMatchers with BeforeAndAfter {

  before {
    // Set up the spark context
    common.init(new SparkConf().setAppName("ex14").setMaster("local[*]"))
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(common.sc)
    val baseDir = new File(PathUtils.getTestDir(getClass()), "miniHive")
    hiveContext.setConf("hive.metastore.warehouse.dir", baseDir.getAbsolutePath())
    common.sqlc = hiveContext
  }

  test("test hive") {
    val test_df = ex10.jsonToDataframe(getClass.getResource("/gb_books.json.gz").getPath)
    ex14.createTableAs(test_df, "ex14")
    ex14.findAuthorWithMostBooks("ex14") must be(("Various", 3228))
  }

  after {
    common.stop
  }

}