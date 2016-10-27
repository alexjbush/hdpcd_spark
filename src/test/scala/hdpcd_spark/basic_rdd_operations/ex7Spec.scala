package hdpcd_spark.basic_rdd_operations

import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

class ex7Spec extends FunSuite with MustMatchers with BeforeAndAfter {

  before {
    // Set up the spark context
    common.init(new SparkConf().setAppName("ex7").setMaster("local[*]"))
  }

  test("test line split function") {
    val test_line = "One two-three four--five six'sevEn"
    ex7.wordsInString(test_line) must be(Array("one", "two-three", "four", "five", "six", "seven"))
  }

  test("test rdd function") {
    val test_rdd = ex5.readFile(getClass.getResource("/mobydick/2701-0.txt").getPath)
    ex7.getWordcount(test_rdd).sortBy(_._2, false).first._1 must be("the")
  }

  after {
    common.stop
  }

}