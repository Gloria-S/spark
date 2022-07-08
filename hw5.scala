import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{Pipeline,PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer,HashingTF, Tokenizer}
import org.apache.spark.sql.functions;
import spark.implicits._

//将数据集存入hdfs
./bin/hdfs dfs -put /home/hadoop/hw/hw4-5/letter-recognition.data.txt .

case class letter(features: org.apache.spark.ml.linalg.Vector, label: String)

//获取训练集测试集
val df = sc.textFile("letter-recognition.data.txt").map(_.split(",")).map(p =>letter(org.apache.spark.ml.linalg.Vectors.dense(
    p(1).toDouble,p(2).toDouble,p(3).toDouble, p(4).toDouble,p(5).toDouble,p(6).toDouble, p(7).toDouble,p(8).toDouble,p(9).toDouble,
    p(10).toDouble,p(11).toDouble,p(12).toDouble,p(13).toDouble,p(14).toDouble,p(15).toDouble,p(16).toDouble), p(0).toString())).toDF()
val train=df.limit(16000)
import org.apache.spark.sql.types.{StructType,StructField,IntegerType,StringType}
val schema = df.schema
val trainSeq=df.collect.toSeq.reverse.take(4000)
val trainRDD=sc.parallelize(trainSeq)
val test=spark.createDataFrame(trainRDD,schema)

//PCA降维
val pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(12).fit(train)
val result = pca.transform(train)
val testdata = pca.transform(test)
result.show()
testdata.show()

//训练分类模型并预测

val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(result)
//labelIndexer.labels.foreach(println)
val featureIndexer = new VectorIndexer().setInputCol("pcaFeatures").setOutputCol("indexedFeatures").fit(result)
//println(featureIndexer.numFeatures)
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
//逻辑回归
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionModel
//import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
val lr = new LogisticRegression().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxIter(100)
val lrPipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))
val lrPipelineModel = lrPipeline.fit(result)
val lrModel = lrPipelineModel.stages(2).asInstanceOf[LogisticRegressionModel]
//println("Coefficients: " + lrModel.coefficientMatrix+"Intercept:"+lrModel.interceptVector+"numClasses: "+lrModel.numClasses+"numFeatures:"+lrModel.numFeatures)
val lrPredictions = lrPipelineModel.transform(testdata)
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
val lrAccuracy = evaluator.evaluate(lrPredictions)
println("Test Error = " + (1.0 - lrAccuracy))
//决策树
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxDepth(20).setImpurity("entropy")
val dtPipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
val dtPipelineModel = dtPipeline.fit(result)
val dtModel = dtPipelineModel.stages(2).asInstanceOf[DecisionTreeClassificationModel]
val dtPredictions = dtPipelineModel.transform(testdata)
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
val dtAccuracy = evaluator.evaluate(dtPredictions)
println("Test Error = " + (1.0 - dtAccuracy))
println("Classification tree model:\n" + dtModel.toDebugString)
//随机森林
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.RandomForestClassificationModel
val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(20).setMaxDepth(20).setImpurity("Entropy")
val rfPipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
val rfPipelineModel = rfPipeline.fit(result)
val rfModel = rfPipelineModel.stages(2).asInstanceOf[RandomForestClassificationModel]
val rfPredictions = rfPipelineModel.transform(testdata)
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
val rfAccuracy = evaluator.evaluate(rfPredictions)
println("Test Error = " + (1.0 - rfAccuracy))
//朴素贝叶斯
//需要非负特征，不使用

//超参数调优,选随机森林
import org.apache.spark.ml.tuning.{CrossValidator,ParamGridBuilder}
val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setImpurity("Entropy")
val rfPipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
val paramGrid = new ParamGridBuilder().addGrid(rf.numTrees,Array(10,30,50)).addGrid(rf.maxDepth,Array(5,10,20)).build()
//val paramGrid = new ParamGridBuilder().addGrid(rf.setMinInfoGain,Array(0.0,0.05)).build()
val cv = new CrossValidator().setEstimator(rfPipeline).setEvaluator(
    new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")).setEstimatorParamMaps(paramGrid).setNumFolds(3)
val cvModel = cv.fit(result)
val rfPredictions =cvModel.transform(testdata)
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
val rfAccuracy = evaluator.evaluate(rfPredictions)
//最多设置参数为numTrees=50,maxDepth=20

val bestModel= cvModel.bestModel.asInstanceOf[PipelineModel]
val rfModel = bestModel.stages(2).asInstanceOf[RandomForestClassificationModel]
println(rfModel.extractParamMap)

//MySQL统计
//改用mysql驱动的spark-shell，换spark以后重新跑了一遍超参数调优
./bin/spark-shell \
--jars /usr/local/spark/jars/mysql-connector-java-8.0.28/mysql-connector-java-8.0.28.jar \
--driver-class-path  /usr/local/spark/jars/mysql-connector-java-8.0.28/mysql-connector-java-8.0.28.jar

//获得最佳模型后提取预测结果
val col=Seq("label","predictedLabel")
val bestModelResult=rfPredictions.select(col.head,col.tail:_*)
//设置连接数据库属性
import java.util.Properties
val prop = new Properties()
prop.put("user", "root") //表示用户名是root
prop.put("password", "hadoop") //表示密码是hadoop
prop.put("driver","com.mysql.jdbc.Driver")//表示驱动程序是com.mysql.jdbc.Driver
//mySQL操作，建立表
create database classification;
use classification;
create table letter (label char(4), predictedLabel char(4));
//从spark-shell写入mysql
bestModelResult.write.mode("append").jdbc("jdbc:mysql://localhost:3306/classification", "classification.letter", prop)
//mysql打印测试集结果
select * from letter;
//统计各个字母的样本数和预测结果数
select label,count(label) from letter group by label;
select predictedLabel,count(predictedLabel) from letter group by predictedLabel;
