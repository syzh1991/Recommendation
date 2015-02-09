import org.apache.spark._
import SparkContext._
import scala.collection.mutable.Map
/**
 * Created by zhang on 2015/1/31.
 */
object Parse extends App{
  System.setProperty("hadoop.home.dir","C:\\Users\\zhang\\Downloads\\hadoop")
  System.setProperty("HADOOP_USER_NAME","root")

  val conf = new SparkConf()
      //.setMaster("spark://naruto.ccntgrid.zju.edu:7077")
      .setMaster("local")
      .setAppName("SparkSQL")
  //.setJars(Seq(System.getenv("SPARK_TEST_JAR")))
  //.setSparkHome(System.getenv("SPARK_HOME"))
  //.set("spark.executor.memory", "512m")
  //.set("spark.executor.core", "1")
  val sc = new SparkContext(conf)
  //val textFile1 = sc.textFile("hdfs://naruto.ccntgrid.zju.edu:8020/user/hive/warehouse/recommendation.db/orderdata/OrderData_Export.csv")
  //val result1 = textFile1.map(line => line.split(",")).map(array =>(array(14).substring(1,array(14).length-1)+","+array(3).substring(1,array(3).length-1),1)).reduceByKey((a,b) => a+b)
  //result1.saveAsTextFile("hdfs://naruto.ccntgrid.zju.edu:8020/user/test/result1")
  val textFile2 = sc.textFile("hdfs://naruto.ccntgrid.zju.edu:8020/user/test/result1")
// (zipcode,productid,9)
  //val regex = """\(\d+,\d+,\d+\)""".r
  val result2 = textFile2
    .map(line => line.split(","))
    .map(array => (array(0).substring(1),array(1)+" "+array(2).substring(0,array(2).length-1)))
    .reduceByKey((a,b) => a+"\002"+b)
  .map(x => (x._1,x._2.split("\002").sortBy(y => y.split(" ")(1).toInt).reverse.toSeq.mkString("\001")))
  .map(z => (z._1,z._2.split("\001").map(s=>s.split(" ")(0)).mkString(" "))).map(x => x._1 + "," + x._2)
  result2.saveAsTextFile("hdfs://naruto.ccntgrid.zju.edu:8020/user/test/result2")
  //val textFile3 = sc.textFile("hdfs://naruto.ccntgrid.zju.edu:8020/user/test/result2")
  //val result3 = textFile3
    // (95388,2118 3,2117 5,19697 1,27174 2,2116 5,26595 2)
    //.map(x => (x._1, x._2.split(',').map(y => y.split(' ')))).foreach(println)
  // (95388,List(List(2118,3),list(2117,5)..))
  //.map(x => (x._1, x._2.toSeq.sortBy(y => y(1)))).foreach(println)


}



