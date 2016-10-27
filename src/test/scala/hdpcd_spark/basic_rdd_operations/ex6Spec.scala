package hdpcd_spark.basic_rdd_operations

import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

class ex6Spec extends FunSuite with MustMatchers with BeforeAndAfter {


  before {
    // Set up the spark context
    common.init(new SparkConf().setAppName("ex6").setMaster("local[*]"))
  }

  test("test persist") {
    val test_rdd = ex5.readFile("hdfs://localhost:8020/moby.txt")
    ex6.persistToMemory(test_rdd).getStorageLevel must be(StorageLevel.MEMORY_ONLY)
  }

  after {
    common.stop
  }

}