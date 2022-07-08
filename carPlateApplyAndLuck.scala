import spark.implicits._
import org.apache.spark.sql.DataFrame
//从本地读取数据
val applyNumbersDF:DataFrame=spark.read.parquet("file:///home/hadoop/hw/hw3/dataset/apply")
val luckyNumbersDF:DataFrame=spark.read.parquet("file:///home/hadoop/hw/hw3/dataset/lucky")

//2016年以后申请、中签总名单
val applyAfter2016=applyNumbersDF.filter("batchNum>201600")
val luckyAfter2016=luckyNumbersDF.filter("batchNum>201600")

//每月抽签总副本数
val applyNum=applyAfter2016.groupBy("batchNum").count().withColumnRenamed("count","applyTotal") 
//applyNum.orderBy("batchNum").show()
//每月中签总人数
val luckNum=luckyAfter2016.groupBy("batchNum").count().withColumnRenamed("count","luckTotal")
//luckNum.orderBy("batchNum").show()
//合并每月申请、中签总人数
val total=applyNum.join(luckNum,Seq("batchNum"),"inner").orderBy("batchNum")

//倍率统计
val applyInfo=applyAfter2016.groupBy("carNum","batchNum").count().withColumnRenamed("count","rate").orderBy("batchNum")
//中签名单中对应倍率
val luckInfo=applyInfo.join(luckyAfter2016,Seq("carNum","batchNum"),"left")

//每个月同倍率申请人数
val applyRateNum=applyInfo.groupBy("batchNum","rate").count().orderBy("batchNum").withColumnRenamed("count","applyRateNum")
//统计每个月同倍率中签人数
val luckRateNum=luckInfo.groupBy("batchNum","rate").count().orderBy("batchNum").withColumnRenamed("count","rateNum")
//对应每个月倍率，中签人数，申请人数
val luckRateInTotal=luckRateNum.join(total,Seq("batchNum"),"inner")
//统计每个月不同倍率中签比例
val luckProportion=luckRateInTotal.withColumn("proportion",col("luckTotal")/col("applyTotal")*col("rateNum")).orderBy("batchNum").drop("luckTotal").drop("applyTotal")
//合并申请而没中签的倍率与所有倍率对应申请人数和中签人数
val statistics=applyRateNum.join(total,Seq("batchNum"),"left").join(luckProportion,Seq("batchNum","rate"),"left").orderBy("batchNum")
//计算每个倍率下个人的理论中签概率
val luckProbability=statistics.withColumn("probability",col("luckTotal")/col("applyTotal")*col("rate")).orderBy("batchNum")

//luckProbability.coalesce(1).write.csv("file:///home/hadoop/hw/hw3/statistics")

//统计申请者倍率变化
val carRate=applyInfo.orderBy("carNum")
val carApplyNum=carRate.groupBy("carNum").count().withColumnRenamed("applyTimes")  //每辆车总共申请次数
