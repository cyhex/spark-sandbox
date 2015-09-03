package de.cms

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphApp {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Graph")
    val sc = new SparkContext(conf)

    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )

    val edgeArray = Array(
      Edge(2L, 1L, "asdasd"),
      Edge(2L, 4L, "asdasd"),
      Edge(3L, 2L, "asdasd"),
      Edge(3L, 6L, "asdasd"),
      Edge(4L, 1L, "asdasd"),
      Edge(5L, 2L, "asdasd"),
      Edge(5L, 3L, "asdasd"),
      Edge(5L, 6L, "asdasd")
    )

    val vertexRDD: RDD[(VertexId, (String, PartitionID))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[String]] = sc.parallelize(edgeArray)
    val graph: Graph[(String, Int), String] = Graph(vertexRDD, edgeRDD)
    val map: RDD[String] = graph.triplets.map(t => t.srcAttr._1 + " => " + t.attr + " : " + t.dstAttr._1)
    println(map.collect().mkString("\n"))

  }
}
