package hdpcd_spark.basic_rdd_operations

import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Accumulator

class ex9Spec extends FunSuite with MustMatchers with BeforeAndAfter {

  before {
    // Set up the spark context
    common.init(new SparkConf().setAppName("ex9").setMaster("local[*]"))
  }

  test("broadcast an array") {
    val test_array = Array("ahab", "ishmael", "queequeg", "pip", "moby")
    ex9.broadcastArray(test_array).value must be(test_array)
  }

  test("test top hits with aggregator") {
    val test_rdd = ex5.readFile(getClass.getResource("/mobydick/2701-0.txt").getPath)
    val rdd = ex7.getWordcount(test_rdd)
    val accum = common.sc.accumulator[Long](0, "test")
    val test_array = Array("ahab", "ishmael", "queequeg", "pip", "moby")
    val bc_test_array = ex9.broadcastArray(Array("ishmael", "queequeg", "pip", "moby"))
    val result = ex8.getWordcount(rdd, ex9.filterInArray(bc_test_array, Option(accum)), 2)
    result must be(Array("queequeg", "moby"))
    accum.value must be(437226)
  }

  after {
    common.stop
  }

}