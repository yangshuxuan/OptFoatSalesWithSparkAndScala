package wuxi99
import org.apache.spark.SparkConf
import com.databricks.spark.avro._
import org.apache.spark.sql._
import scala.util.matching.Regex
import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import scala.collection.JavaConversions._
import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.rdd.RDD
//import org.joda.time.{DateTime, Interval,LocalDate,LocalTime}
//import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._
import java.io.FileNotFoundException
import MailUtility.{sendMail}
import grizzled.slf4j.Logger

class DeviceInfoDataFrame(val deviceInfo:DataFrame) {
  import DeviceInfoDataFrame.deleteHdfsTempFile
  /**** 用字典进行校正 *****/
  def correctByDict(destFiledName : String,useFiledName:String,dict:Map[String,String] ) = {
    val correctMethod =  udf((v: String,w:String ) => dict.getOrElse(v,w))
    deviceInfo.withColumn(destFiledName,correctMethod(deviceInfo(useFiledName),deviceInfo(destFiledName)))
  }
  private def dealWithNull(f:String => String):String => String = {
    val p:String => String = {
          case null => "0"
          case v:String => f(v)}
    p
  }
  /*** 用正则表达式进行校正 ****/
  def correctByRegular(destFiledName:String,r:Regex) = {
    //val f:String => String = {
    //                        case null => "0"
    //                        case v:String => r findFirstIn v match {
    //                          case Some(x) => x
    //                          case None => "0" }}
    //val correctMethod =  udf(f)
    val correctMethod =  udf(dealWithNull((v: String) =>  r findFirstIn v match {
                              case Some(x) => x
                              case None => "0" }))
    deviceInfo.withColumn(destFiledName,correctMethod(deviceInfo(destFiledName)))
  }
  def correctBrands() = {
    val destFiledName = "brands"
    val brandsDict = Map("中国电信" -> "46005","中国移动" -> "46007" ,"中国联通" -> "46001", "未知" -> "unknown") ++ List("蜗牛移动","优友","京东通信","红豆电信","中麦通信","迪加通信","阿里通信").map( _ -> "0").toMap
    val brandsRegr  = brandsDict.keys.map("(" + _ + ")").mkString("|").r
    val numRegr = "^[0-9]+$".r
    val correctMethod =  udf(dealWithNull((v: String) => brandsRegr findFirstIn  v  match {
                            case Some(x) => brandsDict(x)
                            case None => {
                                numRegr findFirstIn  v  match {
                                  case Some(y) if y.length > 7  => y.substring(0,5)
                                  case _ => v
                                } 
                            }
                           }))
    deviceInfo.withColumn(destFiledName,correctMethod(deviceInfo(destFiledName)))
  }
  def unionByName(b: DataFrame): DataFrame = {  
    val columns = deviceInfo.columns.toSet.intersect(b.columns.toSet).map(col).toSeq  
    deviceInfo.select(columns: _*).unionAll(b.select(columns: _*))
  }
  /*不知为何unionAll会改变字段类型*/
  def correctDataType() ={
    deviceInfo.
    withColumn("up_time",deviceInfo("up_time").cast("float")). //与主流设备整合字段类型
    withColumn("last_checktime",deviceInfo("last_checktime").cast("float")). //与主流设备整合字段类型
    withColumn("istest_device",deviceInfo("istest_device").cast("int")). //与主流设备整合字段类型
    withColumn("is_push",deviceInfo("is_push").cast("int")) //与主流设备整合字段类型
  }
  def addCityEnColumn(chinaCityDF:DataFrame) ={
    val chinaCityDFRenamed = chinaCityDF.withColumnRenamed("city_zh","city_zh_dict")
    deviceInfo.join(chinaCityDFRenamed,deviceInfo("city_zh") === chinaCityDFRenamed("city_zh_dict"),"left_outer").drop("city_zh_dict")
  }
  def addSim2CarrierColumn(operatorDF:DataFrame) ={
    deviceInfo.join(operatorDF,deviceInfo("secondOperator") === operatorDF("mcc_mac"),"left_outer").drop("mcc_mac").withColumnRenamed("operator_en","sim2_carrier")
  }
  def distinctByImei(pt:String = "2017-02-07")(implicit sqlHiveContext: HiveContext)  = {
    import sqlHiveContext.implicits._
    //重新排列imei和imei2,小的在前面大的在后面，便于去重
    val getMin =  udf((v: String,w:String ) => if(v <= w) v  else w )   //取小的imei
    val getMax =  udf((v: String,w:String ) => if(v > w) v  else w )    //取大的imei
    val imeiHistoryReOrder =  deviceInfo.withColumnRenamed("imei","oldimei").withColumnRenamed("imei2","oldimei2").
                    withColumn("imei",getMin($"oldimei",$"oldimei2")).
                    withColumn("imei2",getMax($"oldimei",$"oldimei2")).
                    drop("oldimei").drop("oldimei2")

    //历史去重
    val historyImei = sqlHiveContext.sql(s"FROM ct_fota.tmp_history_imei SELECT * where pt < '${pt}'").drop("pt").drop("imei").cache
    //val historyImei = sqlHiveContext.sql(s"FROM ct_fota.tmp_history_imei SELECT * where pt < '${pt}'").drop("pt").drop("imei").drop("oem").drop("product").drop("region").drop("operator").distinct.cache
    val imeiHistoryDedup = imeiHistoryReOrder except  imeiHistoryReOrder.join(historyImei,Seq("oem","product","region","operator","imei2")).cache  //减掉能够join成功的，结果就是历史去重
    //val imeiHistoryDedup = imeiHistoryReOrder except  imeiHistoryReOrder.join(historyImei,Seq("imei2")).cache  //减掉能够join成功的，结果就是历史去重

    //当日去重
    imeiHistoryDedup.dropDuplicates(Seq("oem","product","region","operator","imei2"))
    //imeiHistoryDedup.dropDuplicates(Seq("imei2"))
    
  }

  def distinctByMid(pt:String = "2017-02-07")(implicit sqlHiveContext: HiveContext)  = {
    val historyMid = sqlHiveContext.sql(s"FROM ct_fota.tmp_history_mid SELECT * where pt < '${pt}'").drop("pt").cache //读取历史mid

    val midHistoryDedup = deviceInfo except  deviceInfo.join(historyMid,Seq("oem","product","region","operator","mid"))

    //midHistoryDedup.show

    midHistoryDedup.dropDuplicates(Seq("oem","product","region","operator","mid"))
  }
  def correctForMdstallpro1stimeipt()(implicit sqlHiveContext: HiveContext)  ={
    import sqlHiveContext.implicits._
  //在将imei去重后的数据存放到表ysx_md_st_all_pro1st_imei_pt之前，还必须进行重命名，真是无处不在的坑
    val fillZero =  udf(() => "0")
    val lastNames =  List("project_code","md5_code","seq_row","import_time","col5","last_ip","last_mac","last_sn",  "last_sim",  "last_time", 
                    "last_resolution",  "last_brands",  "last_release",  "last_language",
                    "last_app_version",  "last_sdk_version",  "last_imsi",  "last_operator_name",
                    "last_main_gid",  "last_second_gid",  "last_second_operator",  "last_imei2",  
                    "last_sim2_mac",  "last_sim2_carrier",  "last_esn",  "last_current_spn",  "last_minor_spn",
                    "last_minor_imsi",  "last_continent_en",  "last_continent_zh",  "last_country_en",
                    "last_country_zh",  "last_province_en",  "last_province_zh",  "last_ctiy_en",  "last_city_zh",  "last_country_type")
    
    lastNames.foldLeft(deviceInfo)((u,v) => u.withColumn(v, fillZero())).
    drop("mid").
    withColumnRenamed("apntype","apn_type").
    withColumnRenamed("appversion", "app_version").
    withColumnRenamed("city_en", "ctiy_en").
    withColumnRenamed("citycode","city_code").
    withColumnRenamed("devicesinfoext","devicesinfo_ext").
    withColumnRenamed("devicetype","device_type").
    withColumnRenamed("maingid","main_gid").
    withColumnRenamed("networktype","network_type").
    withColumnRenamed("platform","phone_platform").
    withColumn("sim2_mac",$"secondoperator").
    withColumnRenamed("secondoperator","second_operator").
    withColumnRenamed("gcmid","col4").
    withColumnRenamed("co7","col6").
    withColumnRenamed("co8","col7").
    withColumnRenamed("co9","col8").
    withColumnRenamed("provincecode","province_code").
    withColumnRenamed("rebootappversion","reboot_app_version").
    withColumnRenamed("rebootsign","reboot_sign").
    withColumnRenamed("sdkversion","sdk_version").
    withColumnRenamed("selfsign","self_sign").
    withColumnRenamed("sim1_spn","current_spn").
    withColumnRenamed("sim2_spn","minor_spn").
    withColumnRenamed("sim2_imsi","minor_imsi").
    withColumnRenamed("sim1_carrier","operator_name").
    withColumnRenamed("secondgid","second_gid").
    withColumn("last_checktime",from_unixtime($"last_checktime")).
    withColumn("is_push",when($"is_push" === 0,"FALSE").otherwise("TRUE")).
    withColumn("istest_device",when($"istest_device" === 0,"FALSE").otherwise("TRUE")).
    withColumn("up_time",$"up_time".cast("string"))
  }
  def saveCommon(pt:String = "2017-02-07",destTableName:String)(implicit sqlHiveContext: HiveContext)  {
    val path = s"tmp/${destTableName}/pt=${pt}"
    deleteHdfsTempFile(path)
    deviceInfo.write.avro(path)                                           //存储清理结果到hdfs
    val loadCMD = s"LOAD DATA  INPATH '${path}' OVERWRITE INTO TABLE ct_fota.${destTableName} PARTITION (pt='${pt}')"
    sqlHiveContext.sql(loadCMD)
    
  }
  
  def correctColumnName()={
    //用于对fota3,fota4,fota5列名进行清理和校验，
    deviceInfo.drop("id").
    withColumnRenamed("CO1","sim1_spn").
    withColumnRenamed("CO2","sim2_spn").
    withColumnRenamed("CO3","sim2_imsi").
    withColumnRenamed("CO4","imei2").
    withColumnRenamed("plat","sim1_carrier").
    withColumnRenamed("CO5","esn").withColumnRenamed("CO6","gcmid")
  }
  def correctFota5AddedColumnName() = {
    //由于fota3,fota4没有这三个字段，那就利用co10,co11,co12转变过来
    deviceInfo.withColumnRenamed("CO10","rebootSign").
    withColumnRenamed("CO11","selfSign").
    withColumnRenamed("CO12","rebootAppVersion")
  }
  def correctUpperToLower() = {
    deviceInfo.
    withColumnRenamed("APNType","apntype"). //将avro加载到hive时,必须先将字段名小写 :)
    withColumnRenamed("provinceCode","provincecode").
    withColumnRenamed("cityCode","citycode").
    withColumnRenamed("RELEASE","release").
    withColumnRenamed("deviceType","devicetype").
    withColumnRenamed("devicesinfoExt","devicesinfoext").
    withColumnRenamed("networkType","networktype").
    withColumnRenamed("secondOperator","secondoperator").
    withColumnRenamed("CO7","co7").
    withColumnRenamed("CO8","co8").
    withColumnRenamed("CO9","co9").
    withColumnRenamed("rebootSign","rebootsign").
    withColumnRenamed("selfSign","selfsign").
    withColumnRenamed("rebootAppVersion","rebootappversion")
  }
  def countImeiOrMid(fieldName:String,aliasName:String)(implicit groupByNames:Array[String])={
    deviceInfo.groupBy(groupByNames.map(deviceInfo(_)):_*).agg(count(deviceInfo(fieldName)) as aliasName)
  }
  def countImeiOrMidNew(fieldName:String,aliasName:String,otherName:String)(implicit groupByNames:Array[String])={
    val fillZero =  udf(() => 0)
    deviceInfo.groupBy(groupByNames.map(deviceInfo(_)):_*).agg(count(deviceInfo(fieldName)) as aliasName).
    withColumn(otherName,fillZero())
  }
  def sumImeiAndMid(imeiFieldName:String,midFieldName:String)(implicit groupByNames:Array[String])={
    deviceInfo.groupBy(groupByNames.map(deviceInfo(_)):_* ).agg(
                sum(deviceInfo(imeiFieldName)) as imeiFieldName,
                sum(deviceInfo(midFieldName)) as midFieldName
    )
  }
  def correctFieldNameOfResultDataSet()={
  //由于从数据库拉取的字段名与最终的结果数据集还是有差异的，有必要重命名

    val connectMethod =  udf((oem: String,product:String,region:String,operator:String ) =>  List(oem,product,region,operator).mkString("_")) // 用来将imei imei2连接起来，规则是小的在前，大的在后
    deviceInfo.
    withColumnRenamed("city_en","ctiy_en"). 
    withColumnRenamed("devicetype","device_type").
    withColumnRenamed("platform","phone_platform").
    withColumnRenamed("sdkversion","sdk_version").
    withColumnRenamed("sim1_carrier","operator_name").
    withColumnRenamed("appversion","app_version").
    withColumn("project_code",connectMethod(deviceInfo("oem"),
      deviceInfo("product"),
      deviceInfo("region"),
      deviceInfo("operator")))

  }
  def delRedundant(suffix:String = "a")(implicit groupByNames:Array[String]) = {
    val fillNull = udf((x:String,y:String) => (x,y) match {
      case (null,v) => v
      case (v,null) => v
      case (v,u) => v
      }
    )
    groupByNames.foldLeft(deviceInfo)((u,v) => u.withColumn(v,fillNull(u(v),u(v + suffix))).drop(v+suffix))
  }
  def addSuffix(suffix:String = "a")(implicit groupByNames:Array[String]) = {
      groupByNames.foldLeft(deviceInfo)((u,v) => u.withColumnRenamed(v, v + suffix))
  }

  def buildColumeEquelExp(y:DataFrame,suffix:String = "a")(implicit groupByNames:Array[String]) = {
      groupByNames.map(t => deviceInfo(t) === y(t+suffix)).reduceLeft(_ && _)
  }
    
  
}
object DeviceInfoDataFrame {
  @transient lazy val logger = Logger[this.type]
  implicit def toFrame(e:DeviceInfoDataFrame) = e.deviceInfo
  implicit def toDeviceInfoDataFrame(e:DataFrame) = new DeviceInfoDataFrame(e)
  implicit val groupByNames:Array[String] = Array("oem",  
                  "product",
                  "region",
                  "operator",
                  "origin_version",
                  "resolution",
                  "brands",
                  "release",
                  "language",
                  "appversion",
                  "sdkversion",
                  "sim1_carrier",
                  "platform",
                  "devicetype",
                  "continent_en",
                  "continent_zh",
                  "country_en",
                  "country_zh",
                  "province_en",
                  "province_zh",
                  "city_en",
                  "city_zh",
                  "country_type",
                  "data_version")
  private def readDataFile(pt:String,x:String)(implicit sqlHiveContext: HiveContext) = try {
    Some(sqlHiveContext.read.avro(s"${pt}/${x}"))
  } catch {
    case ex: FileNotFoundException =>  None
  }
  private def deleteHdfsTempFile(path:String){
    //因为是删除临时文件夹，无所谓成功与失败了
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem,Path}
    import java.net.URI
    import org.apache.hadoop.fs.Path
    val protocol:String="hdfs://adups"
    val conf = new Configuration()
    val fileSystem = FileSystem.get(URI.create(protocol),conf)
    fileSystem.delete(new Path(path),true)
  }

  /****加载设备信息****************/
  def loadDeviceInfo(deviceTableNames:List[String],pt:String = "2017-02-07")(implicit sqlHiveContext: HiveContext) = {

        deviceTableNames.map(readDataFile(pt,_)).flatMap(x => x.map( z => z)).reduceLeft( _ unionByName _).
        correctColumnName().
        correctDataType()
  }
  /****加载地区信息***************/
  def loadRegionInfo(regionTableName:String,pt:String = "2017-02-07")(implicit sqlHiveContext: HiveContext) = {
    sqlHiveContext.read.avro(s"${pt}/${regionTableName}").
        withColumnRenamed("ctiy_en","city_en").
        drop("COL3").drop("COL4").drop("COL5").drop("COl2").drop("id").drop("push_time").dropDuplicates(Array("imei","ip","mid"))
  }
  /****加载fota4第三方设备***************/
  def loadOpenDeviceInfo(pt:String = "2017-02-07")(implicit sqlHiveContext: HiveContext) = {
    import sqlHiveContext.implicits._
    sqlHiveContext.read.avro(s"${pt}/v4deviceopen").
        drop("id").
        drop("COl2").
        drop("COL3").
        drop("COL4").
        drop("COL5").
        withColumnRenamed("plat","sim1_carrier").
        withColumnRenamed("ctiy_en","city_en").
        withColumn("push_time",$"push_time".cast("string")) //与主流设备整合字段类型
  }
  
  def cleanFotaSalesData(deviceInfo:DataFrame)(implicit sqlHiveContext: HiveContext) = {

    //加载省份中英文对照表，变成字典
    val chinaProvinceDict = sqlHiveContext.read.avro("/user/hive/warehouse/ct_fota.db/tmp_china_area_province_m2h_01").collect().map { case Row(key: String, value: String) => key -> value}.toMap

    //加载城市中英文对照表，DataFrame
    val chinaCityDF = sqlHiveContext.read.avro("/user/hive/warehouse/ct_fota.db/tmp_china_area_city_m2h_01")

    //加载城市中英文对照表，变成字典
    val chinaCityDict = chinaCityDF.collect().map { case Row(key: String, value: String) => key -> value}.toMap

    //加载运营商代码和名称对照表，DataFrame
    val operatorDF = sqlHiveContext.read.avro("/user/hive/warehouse/ct_fota.db/tmp_md_operator_name_m2h_01")

    //运营商代码和名称对照字典，必须过来掉null值
    val operatorDict =  operatorDF.collect.filter(_.getAs("operator_en") != null).map { case Row(key: String, value: String) => key -> value}.toMap

    val otherREP = new Regex("^[\\w\u4e00-\u9fa5\\pP\\pM\\pZ$|+=]+$")  //可以用来过滤乱码的正则表达式


    //val afterClean = dfDeviceRegion.correctByDict("province_en","province_zh",chinaProvinceDict).  //利用省份的中文名和字典，校正省份的英文名
    deviceInfo.correctByDict("province_en","province_zh",chinaProvinceDict).  //利用省份的中文名和字典，校正省份的英文名
                   correctByDict("city_en","city_zh",chinaCityDict).              //校正城市的英文名
                   correctByDict("sim1_carrier","brands",operatorDict).  //利用brands运营商代码校正第一个运营商名称
                   addSim2CarrierColumn(operatorDF).                     //利用第二个手机卡的运营商代码校正第二个运营商的英文名称
                   correctByRegular("imei","""^[/0-9a-zA-Z_:-]+$""".r).  //过滤imei，注意允许:导致mac地址也作为imei
                   correctByRegular("ip","""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""".r). //过滤ip，注意此处仅仅限制了数字的个数
                   correctByRegular("mac",List.fill(6)("[0-9A-Fa-f]"*2).mkString("^",":","$").r).  //过滤mac地址
                   correctByRegular("sn","""^[A-Za-z0-9]+$""".r).
                   correctByRegular("province",otherREP).                           //过滤乱码
                   correctByRegular("provinceCode",otherREP).
                   correctByRegular("city",otherREP).
                   correctByRegular("cityCode",otherREP).
                   correctByRegular("resolution","[0-9]{3,4}#[0-9]{3,4}".r).
                   correctBrands().                                                 //校正运营商代码
                   correctByRegular("brands","^[\\w+.-]+$".r).
                   correctByRegular("language","^[A-Za-z]{2}_[A-Za-z]{2}$".r).
                   correctByRegular("sdkversion","^[0-9]{1,2}$".r).
                   correctByRegular("imsi","^[0-9]+$".r).
                   correctUpperToLower()
  }
  def computeFotaSalesCount(pt:String = "2017-02-07")(implicit sqlHiveContext: HiveContext){
      import sqlHiveContext.implicits._
          
      ////////////////Fota4/////////////////////////////////////////////////////////////////////////
      val v4dfDeviceInfo = loadDeviceInfo( List("v4device",                                                   //fota4主要设备
        "oldv4device",                                                  //fota4旧设备
        "v4deviceInfo_pad",                                             //fota4Pad设备
        "v4deviceInfoother"),pt).correctFota5AddedColumnName()                                     //加载设备信息

      val v4dfRegion = loadRegionInfo("v4region",pt)                                         //加载地区信息

      val v4DF = v4dfDeviceInfo.join(v4dfRegion,Seq("imei","mid","ip"))

      val v4DFOpenDeviceInfo = loadOpenDeviceInfo(pt).correctFota5AddedColumnName()                             //加载第三方设备信息

      ///////////////Fota5/////////////////////////////////////////////////////////////////////////
      val v5dfDeviceInfo = loadDeviceInfo(List("v5device"),pt)
      val v5dfRegion = loadRegionInfo("v5region",pt)                                         //加载地区信息
      val v5DF = v5dfDeviceInfo.join(v5dfRegion,Seq("imei","mid","ip"))


      //////////////Fota3///////////////////////////////////////////////////////////////////////////
      val v3dfDeviceInfo = loadDeviceInfo(List("v3device"),pt).correctFota5AddedColumnName()
      val v3dfRegion = loadRegionInfo("v3region",pt)                                         //加载地区信息
      val v3DF = v3dfDeviceInfo.join(v3dfRegion,Seq("imei","mid","ip"))
      


      val dfDeviceRegion = (v4DF unionByName v4DFOpenDeviceInfo unionByName v5DF unionByName v3DF).correctDataType().repartition(80).cache   //拼接设备信息和设备信息作为销量数据

      val afterClean = cleanFotaSalesData(dfDeviceRegion).cache                       //清洗销量数据

      //val testFields = afterClean.correctForMdstallpro1stimeipt
      afterClean.saveCommon(pt,"ysx_md_st_all_dt")                                     //存储清理后的销量到hive表里面

      /********去重imei**********************************/
      
      val toDaySalesByImei = (afterClean distinctByImei pt).cache
      toDaySalesByImei.select($"oem",$"product",$"region",$"operator",$"imei",$"imei2").saveCommon(pt,"tmp_history_imei")
      toDaySalesByImei.correctForMdstallpro1stimeipt().saveCommon(pt,"ysx_md_st_all_pro1st_imei_pt")


      /********去重mid***********************************/

      val toDaySalesByMid = (afterClean distinctByMid pt).cache
      toDaySalesByMid.select("oem","product","region","operator","mid").saveCommon(pt,"tmp_history_mid")

      /********旧的计算方式，尤其是表合并非常复杂*************/

      /*val imeit = toDaySalesByImei.countImeiOrMid("imei2","imei_sum")

      val midt = toDaySalesByMid.countImeiOrMid("mid","mid_sum").addSuffix()

      imeit.join(midt,imeit.buildColumeEquelExp(midt),"outer").
      delRedundant().
      correctFieldNameOfResultDataSet().
      saveCommon(pt,"ysx_md_st_all_pro1st_day_pt")*/
      /********新的计算方式*********************************/
      val imeit = toDaySalesByImei.countImeiOrMidNew("imei2","imei_sum","mid_sum")
      val midt = toDaySalesByMid.countImeiOrMidNew("mid","mid_sum","imei_sum")

      imeit.unionByName(midt). 
      sumImeiAndMid("imei_sum","mid_sum").
      correctFieldNameOfResultDataSet().
      saveCommon(pt,"ysx_md_st_all_pro1st_day_pt")
  }
  def compareWithWXJ(pt:String)(implicit sqlHiveContext: HiveContext){
      def computeTotalImei(tableName:String)={
          
          sqlHiveContext.sql(s"FROM ct_fota.${tableName} SELECT * where pt = '${pt}'").agg(sum("imei_sum")).head.getLong(0)
      }

      val newTotalImeiCount = computeTotalImei("ysx_md_st_all_pro1st_day_pt")

      val oldTotalImeiCount = computeTotalImei("md_st_all_pro1st_day_pt")

      val diff = newTotalImeiCount - oldTotalImeiCount
      val diffPerc = 1.0 * diff / oldTotalImeiCount
      val mailTitle = s"销量差异 ${pt}"
      val mailContent = s"优化销量：${newTotalImeiCount}，晓晶销量：${oldTotalImeiCount}，差异：${diff}，差异百分比：${diffPerc}"
      logger.info(mailTitle + "-----" + mailContent)
      sendMail(mailTitle,mailContent)
      assert(diff <= 800)


  }

}
