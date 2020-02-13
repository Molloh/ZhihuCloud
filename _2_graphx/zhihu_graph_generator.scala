import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object zhihu_graph_generator {
  def main(args: Array[String]): Unit = {
    val follow_url = "mongodb://172.19.240.197:20000/CloudComputing.UserFollow"

    val spark: SparkSession = SparkSession.builder()
      .master("spark://172.19.240.197:7077")
      .appName("zhihu_graph_generator")
      .getOrCreate()

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    // 读取MongoDB中的用户关注数据
    val follow_df: DataFrame = spark.read
      .format("com.mongodb.spark.sql")
      .option("spark.mongodb.input.uri", follow_url)
      .load()

    val usr_follow_df: DataFrame = follow_df
      .select("urlToken", "following")
      .withColumn("following", explode(col("following.urlToken")))
    // 生成用户的点
    val usr_vertex: RDD[(VertexId, String)] = usr_follow_df
      .select(explode(array('urlToken, 'following)))
      .distinct()
      .rdd.map(_.getAs[String](0))
      .zipWithIndex()
      .map(_.swap)
    val usr_vertex_df = usr_vertex.toDF("id", "node")
    usr_vertex_df.write.format("com.mongodb.spark.sql").option("spark.mongodb.output.uri", "mongodb://172.19.240.197:20000/CloudComputing.usr_vertex").save()

    // 生成用户关注的边
    val usr_follow_edge: RDD[Edge[String]] = usr_follow_df
      .join(usr_vertex_df, usr_follow_df("urlToken") === usr_vertex_df("node"))
      .select('following, 'id as 'id1)
      .join(usr_vertex_df, usr_follow_df("following") === usr_vertex_df("node"))
      .rdd.map(row => Edge(row.getAs[Long]("id1"), row.getAs[Long]("id"), "following"))

    val usr_follow_edge_df = usr_follow_edge.toDF("srcId", "dstId", "relation")
    usr_follow_edge_df.write.format("com.mongodb.spark.sql").option("spark.mongodb.output.uri", "mongodb://172.19.240.197:20000/CloudComputing.usr_follow_edge").save()

  }
}
