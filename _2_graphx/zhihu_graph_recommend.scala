import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object zhihu_graph_recommend {
  def main(args: Array[String]): Unit = {
    val usr_vertex_url = "mongodb://172.19.240.197:20000/CloudComputing.usr_vertex"
    val usr_follow_edge_url = "mongodb://172.19.240.197:20000/CloudComputing.usr_follow_edge"

    val spark: SparkSession = SparkSession.builder()
      .master("spark://172.19.240.197:7077")
      .appName("zhihu_graph_recommend")
      .getOrCreate()
    val sqlContext = spark.sqlContext
    val sparkContext = spark.sparkContext
    import sqlContext.implicits._

    // 读取存储在MongoDB中的表，生成点和边
    val usr_vertex_df = spark
      .read
      .format("com.mongodb.spark.sql")
      .option("spark.mongodb.input.uri", usr_vertex_url)
      .load()
    val usr_follow_edge_df = spark
      .read
      .format("com.mongodb.spark.sql")
      .option("spark.mongodb.input.uri", usr_follow_edge_url)
      .load()
    val usr_vertex: RDD[(VertexId, String)] = usr_vertex_df.rdd.map(x => (x(1).asInstanceOf[Long], x(2).asInstanceOf[String]))
    val usr_follow_edge: RDD[Edge[String]] = usr_follow_edge_df.rdd.map(x => Edge(x(1).asInstanceOf[Long], x(3).asInstanceOf[Long], x(2).asInstanceOf[String]))
    // 根据点和边生成图
    val graph: Graph[String, String] = Graph(usr_vertex, usr_follow_edge)
    // PageRank算法，计算每个点的权重
    val ranks: VertexRDD[Double] = graph.pageRank(0.0001).vertices

    val usr_with_rank: RDD[(VertexId, (String, Double))] = usr_vertex.join(ranks)

    val rank_df = usr_with_rank.toDF("id", "node")
    rank_df.write.format("com.mongodb.spark.sql").option("spark.mongodb.output.uri", "mongodb://172.19.240.197:20000/CloudComputing.usr_rank").save()

    // 新图，顶点具有权重属性
    val ranked_graph: Graph[(String, Double), String] = Graph(usr_with_rank, usr_follow_edge).cache()
    // 获取每个点的入度出度
    val inDeg = ranked_graph.inDegrees
    val outDeg = ranked_graph.outDegrees
    usr_with_rank.join(inDeg.join(outDeg)).toDF("id", "attr")
      .write.format("com.mongodb.spark.sql").option("spark.mongodb.output.uri", "mongodb://172.19.240.197:20000/CloudComputing.usr_rank_in_out").save()

    // 聚合所有邻居的属性
    val usr_with_neighbor = ranked_graph.ops.collectNeighbors(EdgeDirection.Out)
    // 将聚合后的点生成一张新图
    val second_ranked_graph = Graph(usr_with_neighbor, usr_follow_edge)
    // 聚合新图中的点的邻居属性，即为二跳邻居
    val second_follow = second_ranked_graph.ops.collectNeighbors(EdgeDirection.Out)

    // 处理聚合后的点的属性
    val new_second_follow = second_follow.map(
      x => (x._1, x._2.map(y => y._2.map(z => (z._1, z._2._2))))
    )
    val sf_df = new_second_follow.toDF("id", "attr")
      .withColumn("neighbor", explode(col("attr"))).drop(col("attr"))
      .withColumn("second_neighbor", explode(col("neighbor"))).drop(col("neighbor"))
      .withColumn("sn_id", col("second_neighbor._1"))
      .withColumn("sn_rank", col("second_neighbor._2"))
      .drop("second_neighbor")

    // 对每个用户的二跳邻居的（rank * 出现次数）的值进行排序，将值最大的用户挑选为该用户的推荐关注
    val recommend_df = sf_df.groupBy("id", "sn_id", "sn_rank").count().withColumn("weight", col("sn_rank") * col("count"))
      .groupBy("id").agg(max("weight"), first("sn_id") as "sn_id")
    val follow_recommend_edge: RDD[Edge[String]] = recommend_df.rdd.map(x => Edge(x(0).asInstanceOf[Long], x(2).asInstanceOf[Long], "recommend"))
    follow_recommend_edge.toDF("srcId", "dstId", "relation")
      .write.format("com.mongodb.spark.sql").option("spark.mongodb.output.uri", "mongodb://172.19.240.197:20000/CloudComputing.recommend_edge").save()

  }
}
