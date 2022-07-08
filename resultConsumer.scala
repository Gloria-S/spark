package org.apache.spark.examples.streaming
import org.apache.spark._
import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext._

object KafkaResultConsumer{

  def main(args:Array[String]){
    StreamingSet.setStreamingLogLevels()
    val conf=new SparkConf().setMaster("local[*]").setAppName("KafkaRating")
    val spark:SparkSession =SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    val sqlContext=new SQLContext(sc)
    val ssc = new StreamingContext(sc,Minutes(1))
    
    ssc.checkpoint("file:///usr/local/spark/mycode/kafka/checkpoint") //设置检查点
    val zkQuorum = "localhost:2181" //Zookeeper服务器地址
    val group = "1"  //topic所在的group，可以设置为自己想要的名称
    val topics = "result"  //topics的名称          
    val numThreads = 1  //每个topic的分区数
    val topicMap =topics.split(",").map((_,numThreads.toInt)).toMap
    val ds = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)
    val linesMap=ds.window(Minutes(2),Minutes(2)).map(_._2)
    val df=linesMap.map(row=>row.split("#")).map(line=>(line(0),line(1),line(2),line(3)))
    import spark.implicits._
    
    df.foreachRDD{line=>
      val titleDF=line.toDF.withColumnRenamed("_1","title").withColumnRenamed("_2","rating").withColumnRenamed("_3","genres").withColumnRenamed("_4","counts")
      val counts=titleDF.select("genres","counts").dropDuplicates("genres")
      counts.show()
      val movieNum=titleDF.groupBy("genres").count().withColumnRenamed("count","movieNum")
      movieNum.show()
      titleDF.drop("counts").show(titleDF.count.toInt,false)
    }
    
    //linesMap.print
    
    ssc.start
    ssc.awaitTermination
  }
}