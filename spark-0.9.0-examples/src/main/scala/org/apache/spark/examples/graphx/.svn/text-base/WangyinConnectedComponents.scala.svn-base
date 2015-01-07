package org.apache.spark.examples.graphx

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.Analytics

object WangyinConnectedComponents {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: WangyinConnectedComponents <master> <edge_list_file>\n" +
          "    [--numIter=<num_of_iterations>]\n" +
          "        The number of iterations.\n" +
          "    [--output=<output_file>]\n" +
          "        If specified, the file to write the ranks to.\n" +
          "    [--dynamic=<is_dynamic>]\n" +
          "        If specified, iteration will be auto computed.\n" +
          "    [--numEPart=<num_edge_partitions>]\n" +
          "        The number of partitions for the graph's edge RDD. Default is 4.\n" +
          "    [--numVPart=<num_vertex_partitions>]\n" +
          "        The number of partitions for the graph's vertex RDD. Default is 4.\n" +
          "    [--partStrategy=RandomVertexCut | EdgePartition1D | EdgePartition2D | " +
          "CanonicalRandomVertexCut]\n" +
          "        The way edges are assigned to edge partitions. Default is RandomVertexCut.")
      System.exit(-1)
    }

    Analytics.main(args.patch(1, List("cc"), 0))
  }
}