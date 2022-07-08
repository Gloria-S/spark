# 基于Scala的Spark实践项目
1.车牌摇号倍率与中签率数据处理分析\
  使用spark-shell交互式编程，过程代码记录在carPlateApplyAndLuck.scala。\
  由于从2016年开始实行倍率制度，因此数据集从batchNum=201601开始处理，按月统计申请人数和中签人数，计算申请者的倍率系数，
  再统计当月不同倍率的中签情况，分析抽签制度的不足与改进方向。\
2.电影评论数据分析\
  对movie.csv和rating.csv进行数据预处理，spark-shell交互式编程处理的过程代码记录在movieAndRatingDataPreProcessing.scala。\
  movies.csv文件包含3个字段，分别为movieId、title和genres（流派）；
  rating.csv文件共有2000多万条记录，描述了用户对所看电影的评分情况，一共包含4个字段，分别为userId、movieId、rating、timestamp，截取前20万条有电影流派信息的数据。\
  producer.scala生成Kafka数据流，每秒发送100条电影评分信息到Kafka的ratingsender话题下。\
  consumer.scala接收ratingsender话题下的数据流，每两分钟统计过去五分钟内接收到的电影信息中点击量最高的3类电影，输出其中评分最高的电影名称发送到Kafka的result话题下。\
  resultConsumer.scala接收result话题下的数据流并输出。\
  streamingSet.scala设置log4j。\
  用sbt打包程序。\
3.字母识别的多分类任务\
  数据集：https://archive.ics.uci.edu/ml/datasets/Letter+Recognition  
  使用spark-shell交互式编程，过程代码记录在LetterRecognition.scala。\
  使用Spark Mlib调用机器学习模型计算，使用Pipeline构建机器学习工作流。\
  使用PCA进行特征降维处理，调用逻辑回归模型、决策树模型和随机森林模型进行训练，训练accuracy分别为：0.7171，0.7909，0.9153。\
  对随机森林模型进行超参数优化，由于电脑虚拟机硬件限制最高只能调到0.9306，否则会因计算参数太多导致进程崩溃。\
  最佳训练结果写入MySQL数据表进行统计，使用Excel绘制可视化图表。
