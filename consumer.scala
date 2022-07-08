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

object KafkaMRConsumer{

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
    val topics = "ratingsender"  //topics的名称          
    val numThreads = 1  //每个topic的分区数
    val topicMap =topics.split(",").map((_,numThreads.toInt)).toMap
    val ds = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)
    val lines=ds.window(Minutes(5),Minutes(2))
    val linesMap=lines.map(_._2)
    
    val movieRating=linesMap.map(row=>row.split(",")).map(row=>(row(0),row(2))) //(movieId,rating)
    import spark.implicits._
    val genre=spark.read.format("csv").option("header","true").load("movies.csv") //(movieId,title,genres)
    
    movieRating.foreachRDD{rdd=>
      val df=rdd.toDF().withColumn("_2",col("_2").cast(DoubleType))  //"_1":movieId,"_2":rating
      val avgRating=df.groupBy("_1").mean("_2").withColumnRenamed("avg(_2)","avgRating")	//_1:movieId,avgRating
      val movieAvgRatingInfos=avgRating.join(genre,avgRating("_1")===genre("movieId")).drop("_1")	//avgRating,movieId,title,genres
      val movieClickInfos=df.drop("_2").join(genre.drop("title"),df("_1")===genre("movieId")).drop("_1") //.withColumnRenamed("_2","rating")  //movieId,genres
      //movieAvgRatingInfos.show()
      //movieClickInfos.show()
      val genreClickCounts=movieClickInfos.groupBy("genres").count()  //genres,count
      //genreClickCounts.show()
      val topGenreCount=genreClickCounts.orderBy(genreClickCounts("count").desc).limit(3)
      topGenreCount.show()
      val topGenre=topGenreCount.select("genres").withColumnRenamed("genres","topGenre")
      val genreMovies=topGenre.join(movieAvgRatingInfos,topGenre("topGenre")===movieAvgRatingInfos("genres")).drop("genres") //avgRating,movieId,title,genres
      //genreMovies.show()
      val topGenreRating=genreMovies.groupBy("topGenre").max("avgRating").withColumnRenamed("topGenre","genres") //genres,max(avgRating)
      //topGenreRating.show()
      val topRating=topGenreRating.join(genreMovies,topGenreRating("max(avgRating)")===genreMovies("avgRating")
        &&topGenreRating("genres")===genreMovies("topGenre")).drop("genres")
      //topRating.show()
      //title,avgRating,genres,counts
      val result=topRating.select("topGenre","title","avgRating").join(topGenreCount,topRating("topGenre")===topGenreCount("genres")).drop("topGenre").toDF
      result.collect.foreach { line=>
        val props = new HashMap[String, Object]()
        val brokers="localhost:9092"
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        val str=line(0).toString+"#"+line(1).toString+"#"+line(2).toString+"#"+line(3).toString
        val message = new ProducerRecord[String, String]("result", str)
        println(str)
        producer.send(message)
        producer.close()
      }
    }
    
    ssc.start
    ssc.awaitTermination
  }
}  
