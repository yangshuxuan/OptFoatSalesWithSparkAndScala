package wuxi99
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import DeviceInfoDataFrame.{computeFotaSalesCount}
object SimpleApp {
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("You arguments were " + args.mkString("[", ", ", "]"))
      System.err.println(
        """
          |Usage: RecoverableNetworkWordCount <hostname> <port> <checkpoint-directory>
          |     <output-file>. <hostname> and <port> describe the TCP server that Spark
          |     Streaming would connect to receive data. <checkpoint-directory> directory to
          |     HDFS-compatible file system which checkpoint data <output-file> file to which the
          |     word counts will be appended
          |
          |In local mode, <master> should be 'local[n]' with n > 1
          |Both <checkpoint-directory> and <output-file> must be absolute paths
        """.stripMargin
      )
      System.exit(1)
    }
    implicit val sc = {
          val conf = new SparkConf().setAppName("CT FOTA SALES COUNT Application").set("spark.executor.memory", "4g").set("spark.cores.max","60")
          new SparkContext(conf)
      }
    implicit val sqlHiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val Array(pt) = args
    computeFotaSalesCount(pt)
    sc.stop()
  }
}
