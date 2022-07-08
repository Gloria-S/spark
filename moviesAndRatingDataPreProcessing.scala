//spark-shell预处理
//处理movies.csv
val movies=spark.read.format("csv").option("header","true").load("file:///home/hadoop/hw/hw4-5/movies.csv")
val movieInfo=movies.map(row=>{
    val genre=row.getString(2).split("\\|")(0)  //"|"是转义符号
    (row.getString(0),row.getString(1),genre)
}).toDF("movieId","title","genres")
//写入HDFS
movieInfo.coalesce(1).write.option("header","true").csv("movies.csv")
//处理rating.csv
val ratings=spark.read.format("csv").option("header","true").load("file:///home/hadoop/hw/hw4-5/ratings.csv")
val movieWithGenre=movieInfo.filter($"genres"=!="(no genres listed)").select("movieId")
val ratingFilter=ratings.join(movieWithGenre,Seq("movieId"),"inner").limit(200000)
//写入rating-filter.csv
ratingFilter.coalesce(1).write.option("header","true").csv("file:///home/hadoop/hw/hw4-5/rating-filter")


//sbt编译
cd /home/hadoop/spark
/usr/local/sbt/sbt package
//producer运行
cd /usr/local/spark
/usr/local/spark/bin/spark-submit --driver-class-path /usr/local/spark/jars/*:/usr/local/spark/jars/kafka/* --class "org.apache.spark.examples.streaming.KafkaMRProducer" /home/hadoop/spark/target/scala-2.11/hw_2.11-1.0.jar localhost:9092 ratingsender 100
//consumer运行
cd /usr/local/spark
/usr/local/spark/bin/spark-submit --driver-class-path /usr/local/spark/jars/*:/usr/local/spark/jars/kafka/* --class "org.apache.spark.examples.streaming.KafkaMRConsumer" /home/hadoop/spark/target/scala-2.11/hw_2.11-1.0.jar
//resutConsumer运行
cd /usr/local/spark
/usr/local/spark/bin/spark-submit --driver-class-path /usr/local/spark/jars/*:/usr/local/spark/jars/kafka/* --class "org.apache.spark.examples.streaming.KafkaResultConsumer" /home/hadoop/spark/target/scala-2.11/hw_2.11-1.0.jar
