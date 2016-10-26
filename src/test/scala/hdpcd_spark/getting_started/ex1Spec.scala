package hdpcd_spark.getting_started

import org.scalatest._
import org.apache.spark.SparkConf

class ex1Spec extends FunSuite with Matchers {

  val conf = new SparkConf().setAppName("ex1").setMaster("local[*]")

  test("test initilisation") {
    ex1.init(conf)
    ex1.isRunning should be(true)
  }
  
  test("test appname") {
    ex1.getAppName should be("ex1")
  }

  test("test stopping") {
    ex1.stop
    ex1.isRunning should be(false)
  }

}