import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object zhihu_ml {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("zhihu_ml")
      .getOrCreate()

    val usr_question_url = "mongodb://172.19.240.197:20000/CloudComputing.UserQuestionRank"

    val usr_question_df: DataFrame = spark
      .read
      .format("com.mongodb.spark.sql")
      .option("spark.mongodb.input.uri", usr_question_url)
      .load()
      .drop("_id")

    val Array(training, test) = usr_question_df.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setMaxIter(15)
      .setRegParam(0.22)
      .setUserCol("UId")
      .setItemCol("QId")
      .setRatingCol("rate")


    val model = als.fit(training)
    model.setColdStartStrategy("drop")
    val predictions: DataFrame = model.transform(test)

    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rate")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    val userRecs: DataFrame = model.recommendForAllUsers(10)

    val new_recs = userRecs
      .withColumn("new_rec", explode(col("recommendations")))
      .drop("recommendations")
      .withColumn("QId", col("new_rec.QId"))
      .withColumn("Rating", col("new_rec.rating").cast(DoubleType))
      .drop("new_rec")

    new_recs.show(10)
    new_recs.write.format("com.mongodb.spark.sql").option("spark.mongodb.output.uri", "mongodb://172.19.240.197:20000/CloudComputing.recommendations").save()

  }
}