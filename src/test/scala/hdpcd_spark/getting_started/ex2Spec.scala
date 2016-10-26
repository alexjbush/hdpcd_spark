package hdpcd_spark.getting_started

import org.scalatest._
import org.apache.spark.SparkConf

class ex2Spec extends FunSuite with Matchers {

  test("test initilisation of ex2") {
    ex2.initaliseSpark()
    ex1.isRunning should be(true)
  }
  
  test("test appname of ex2") {
    ex1.getAppName should be("ex2")
  }

  test("test stopping of ex2") {
    ex1.stop
    ex1.isRunning should be(false)
  }

}