package hdpcd_spark.basic_rdd_operations

import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.test.PathUtils
import scala.io.Source

import java.io.File

class ex5Spec extends FunSuite with MustMatchers with BeforeAndAfter {

  var hdfsCluster: MiniDFSCluster = null

  before {
    // Set up the spark context
    common.init(new SparkConf().setAppName("ex5").setMaster("local[*]"))

    // Set up HDFS minicluster
    val baseDir = new File(PathUtils.getTestDir(getClass()), "miniHDFS")
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())
    val builder = new MiniDFSCluster.Builder(conf)
    hdfsCluster = builder.nameNodePort(8020).format(true).build()
    hdfsCluster.waitClusterUp()

    // Place file in HDFS
    val fs = FileSystem.get(conf)
    val source = new Path(getClass.getResource("/mobydick/2701-0.txt").getPath)
    fs.copyFromLocalFile(false, false, source, new Path("/moby.txt"))
  }

  test("test sum 2") {
    ex5.countLines("hdfs://localhost:8020/moby.txt") must be(22109)
  }

  after {
    common.stop
    hdfsCluster.shutdown()
  }

}