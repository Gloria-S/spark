//producer.scala
package org.apache.spark.examples.streaming
import java.util.Properties
import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.{SparkSession,Row,DataFrame}
import org.apache.spark.sql.types._

object KafkaMRProducer {
  def main(args: Array[String]) {
    if (args.length < 3) {
        System.err.println("Usage: KafkaProducer <metadataBrokerList> <topic> " +"<messagesPerSec>")
        System.exit(1)
    }
    val Array(brokers, topic, messagesPerSec) = args  //messagesPerSec每秒发送信息数，设100
    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val conf=new SparkConf().setMaster("local[*]").setAppName("producer")
    val sc=new SparkContext(conf)
    val spark:SparkSession =SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    // 发送
    var count=0;
    val ratings=spark.read.format("csv").option("header","true").load("file:///home/hadoop/hw/hw4-5/rating-filter.csv")
    val ratingRDD=ratings.rdd
    ratingRDD.collect.foreach { line=>
      val str=line.toString
      val input=str.substring(1,str.length-1)
      println(input)
      val message = new ProducerRecord[String, String](topic,input)
      producer.send(message)
      count+=1;
      if(count%messagesPerSec.toInt==0){
        Thread.sleep(1000)
        println("\n")
      }
    }
    producer.close()
  }
}
