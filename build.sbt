name := "hdpcd_spark"

version := "1.0"

scalaVersion := "2.11.8"

parallelExecution in Test := false

resolvers += "public.repo.hortonworks.com" at "http://repo.hortonworks.com/content/groups/public/"

def excludeFromAll(items: Seq[ModuleID], group: String, artifact: String) =
  items.map(_.exclude(group, artifact))

def excludeJavaxServlet(items: Seq[ModuleID]) =
  excludeFromAll(items, "javax.servlet", "servlet-api")

libraryDependencies ++= excludeJavaxServlet(Seq(
"org.apache.spark" %% "spark-core" % "1.6.2",
"org.apache.spark" %% "spark-hive" % "1.6.2",
"org.apache.spark" % "spark-sql_2.11" % "1.6.2",
"org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
"org.apache.hadoop" % "hadoop-minicluster" % "2.7.1",
"org.apache.hadoop" % "hadoop-common" % "2.7.1" % "compile,test" classifier "" classifier "tests" excludeAll ExclusionRule(organization = "javax.servlet"),
"org.apache.hadoop" % "hadoop-client" % "2.7.1" % "compile,test" classifier "" classifier "tests" excludeAll ExclusionRule(organization = "javax.servlet"),
"org.apache.hadoop" % "hadoop-hdfs" % "2.7.1" % "compile,test" classifier "" classifier "tests"
))
