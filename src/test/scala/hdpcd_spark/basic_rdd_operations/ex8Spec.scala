package hdpcd_spark.basic_rdd_operations

import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

class ex8Spec extends FunSuite with MustMatchers with BeforeAndAfter {

  before {
    // Set up the spark context
    common.init(new SparkConf().setAppName("ex8").setMaster("local[*]"))
  }

  test("test top hits") {
    val test_rdd = ex5.readFile(getClass.getResource("/mobydick/2701-0.txt").getPath)
    val rdd = ex7.getWordcount(test_rdd)
    val result = ex8.getWordcount(rdd, (t:(String,Int)) => t._1.length > 4, 5 )
    result must be (Array("whale", "there", "which", "their", "other"))
  }

  after {
    common.stop
  }

}