import org.apache.spark._
import SparkContext._

/**
 * Created by zhang on 2015/2/7.
 */
object Process extends App{
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
  val textFile1 = sc.textFile("hdfs://naruto.ccntgrid.zju.edu:8020/user/hive/warehouse/recommendation.db/orderdata/OrderData_Export.csv")
  val result1 = textFile1.map(line => line.split(",")).map(array =>(array(2).substring(1,array(2).length-1)+"\001"+array(3).substring(1,array(3).length-1)+"\001"+array(4).substring(1,array(4).length-1)+"\001"+array(5).substring(1,array(5).length-1)+"\001"+array(14).substring(1,array(14).length-1),1))
    .reduceByKey((a,b) => a+b)
    .map(x=>x._1)
  result1.saveAsTextFile("hdfs://naruto.ccntgrid.zju.edu:8020/user/hive/warehouse/recommendation.db/orderdata_part/a")
}
