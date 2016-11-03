package hdpcd_spark.basic_rdd_operations

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object common {

  // Spark context is shared between all objects
  var sc: SparkContext = _
  
  var sqlc: SQLContext = _

  def init(conf: SparkConf): Unit = { 
    sc = new SparkContext(conf)
    sqlc = new SQLContext(sc)
  }

  def stop(): Unit = sc.stop

}