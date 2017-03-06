package wuxi99
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import DeviceInfoDataFrame.{computeFotaSalesCount}
object CtFotaSalesApp {
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("You arguments were " + args.mkString("[", ", ", "]"))
      System.err.println(
        """
          |Usage: wuxi99.CtFotaSalesApp <日期>. 
          |     <日期> 指定需要计算销量的日期
          |     在该日期之前的销量必须已经计算完毕，否则计算结果错误.
          |
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
