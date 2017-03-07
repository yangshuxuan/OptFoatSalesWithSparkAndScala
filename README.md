# OptFoatSalesWithSparkAndScala
利用spark和scala优化Fota的销量统计
# 执行命令
#spark-submit   --class "wuxi99.CtFotaSalesApp"  --packages com.databricks:spark-avro_2.10:2.0.1  ctfota_2.10-0.9.6.jar <日期> 
spark-submit   --class "wuxi99.CtFotaSalesApp"  --packages com.databricks:spark-avro_2.10:2.0.1,org.clapper:grizzled-slf4j_2.11:1.3.0  ctfota_2.10-0.9.6.jar <日期>
