package hdpcd_spark.basic_rdd_operations

import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class ex4Spec extends FunSuite with MustMatchers with BeforeAndAfter {

  before {
    common.init(new SparkConf().setAppName("ex4").setMaster("local[*]"))
  }

  test("test sum") {
    ex4.sumData must be(108)
  }

  after {
    common.stop
  }

}