package org.apache.spark.examples.graphx

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag
import scala.collection.mutable
import scala.util.hashing._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.graphx.lib.Analytics
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}
import org.apache.spark.storage.StorageLevel._

import java.text.SimpleDateFormat
import java.util.Calendar

object UserRelationInc extends Logging {
  def run[ED: ClassTag](graph: Graph[Long, ED],
    maxIterations: Int = Int.MaxValue): Graph[VertexId, ED] = {
    val ccGraph = graph.mapVertices { case (vid, _) => vid }
    def sendMessage(edge: EdgeTriplet[VertexId, ED]) = {
      if (edge.srcAttr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr))
      } else if (edge.srcAttr > edge.dstAttr) {
        Iterator((edge.srcId, edge.dstAttr))
      } else {
        Iterator.empty
      }
    }
    val initialMessage = Long.MaxValue
    Pregel(ccGraph, initialMessage, maxIterations, activeDirection = EdgeDirection.Either)(
      vprog = (id, attr, msg) => math.min(attr, msg),
      sendMsg = sendMessage,
      mergeMsg = (a, b) => math.min(a, b))
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: UserRelation <master> <table_input_dir> <name>\n" +
          "    [--output=<output_file>]\n" +
          "        If specified, the file to write the ranks to.\n" +
          "    [--outputInc=<output_file>]\n" +
          "        If specified, the file to write the inc ranks to.\n" +
          "    [--groupFieldId=<group field id>]\n" +
          "        comma separated fields of group id for relate pins, e.g. 1,2,3\n" +
          "    [--pinFieldId=<pin field id>]\n" +
          "        field of jd pin.\n" +
          "    [--inputDir=<input Dir>]\n" +
          "        main input dir.\n" +
          "    [--inputIncDir=<input inc Dir>]\n" +
          "        inc input dir.\n" +
          "    [--debug_mode=<debug mode>]\n" +
          "        is debug mode or not.\n" +
          "    [--maxGroupPins=num]\n" +
          "        num of pins in one group.\n" +
          "    [--maxIterations=num]\n" +
          "        num of pins in one group.\n" +
          "    [--partStrategy=RandomVertexCut | EdgePartition1D | EdgePartition2D | " +
          "CanonicalRandomVertexCut]\n" +
          "        The way edges are assigned to edge partitions. Default is RandomVertexCut.")
      System.exit(-1)
    }
    val host = args(0)
    val fname = args(1)
    val options = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    def pickPartitioner(v: String): PartitionStrategy = {
      v match {
        case "RandomVertexCut" => RandomVertexCut
        case "EdgePartition1D" => EdgePartition1D
        case "EdgePartition2D" => EdgePartition2D
        case "CanonicalRandomVertexCut" => CanonicalRandomVertexCut
        case _ => throw new IllegalArgumentException("Invalid PartitionStrategy: " + v)
      }
    }
    val conf = new SparkConf()
    var sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
    conf.set("spark.local.dir", "/mnt/1/yarn/local,/mnt/2/yarn/local,/mnt/3/yarn/local,/mnt/4/yarn/local,/mnt/5/yarn/local,/mnt/6/yarn/local,/mnt/7/yarn/local,/mnt/8/yarn/local,/mnt/9/yarn/local,/mnt/10/yarn/local")
    conf.set("spark.speculation", "true")
    conf.set("spark.akka.threads", "20")
    conf.set("spark.default.parallelism", "100")
    conf.set("spark.storage.memoryFraction", "0.5")
    conf.set("spark.shuffle.copier.threads", "40")
//    conf.set("spark.shuffle.use.netty", "true")

    var outFname = ""
    var partitionStrategy: Option[PartitionStrategy] = None
    var groupFieldId = ""
    var pinFieldId = -1
    var maxGroupPins = 500
    var debug_mode = false
    var maxIterations = Int.MaxValue
    var input_dir = ""
    var input_inc_dir = ""
    var outIncFname = ""

    options.foreach {
      case ("output", v) => outFname = v
      case ("outputInc", v) => outIncFname = v
      case ("partStrategy", v) => partitionStrategy = Some(pickPartitioner(v))
      case ("groupFieldId", v) => groupFieldId = v
      case ("maxGroupPins", v) => maxGroupPins = v.toInt
      case ("pinFieldId", v) => pinFieldId = v.toInt
      case ("debug_mode", v) => debug_mode = v.toBoolean
      case ("maxIterations", v) => maxIterations = v.toInt
      case ("inputDir", v) => input_dir = v
      case ("inputIncDir", v) => input_inc_dir = v
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }
    val sc = new SparkContext(host, "Inc ConnectedComponents(" + fname + ")", conf)
    val input_table = sc.textFile(input_dir)
    val input_edges = input_table.map {
      r => val f = r.split("\\x01|\\s+")
        (f(0), f(1).toLong)
    }.setName("main_input_edges").persist(MEMORY_AND_DISK)
    if (debug_mode) {
      logInfo(input_edges.count().toString())
    }
    val pin_hash_main = input_edges.map {
        case (a, b) => (a.hashCode().toLong, a)
    }.setName("pin_hash_main").persist(MEMORY_AND_DISK)
    if (debug_mode) {
      logInfo(pin_hash_main.count().toString())
    }
    var cal = Calendar.getInstance();
    
    val input_inc_table = sc.textFile(input_inc_dir)
    if (debug_mode) {
      logInfo(sdf.format(cal.getTime()) + " : " + input_inc_table.count().toString())
    }
    val group_pins_inc = input_inc_table.map {
      r => val f = r.split("\\x01|\\s+")
      val key = groupFieldId.split(",").map(id => f(id.trim.toInt)).reduceLeft((res, x) => (res + "," + x))
      (key, f(pinFieldId))
    }.groupByKey().setName("group_pins_inc").persist(MEMORY_AND_DISK)
    if (debug_mode) {
      logInfo(sdf.format(cal.getTime()) + " : " + group_pins_inc.count().toString())
    }

    val pairwise_pins_inc = group_pins_inc.flatMap(
      b => {
		var len : Int = b._2.length
	    val pairs: mutable.Set[(VertexId, VertexId)] = mutable.Set.empty
		if (len < maxGroupPins) {
		  val v = b._2.map(s => s.hashCode())
		  for(i <- 0 to len-2){ 
		    for(j <- i+1 to len-1) {
		      if (v(i).toLong != v(j).toLong) {
                pairs.+= ((v(i).toLong, v(j).toLong))
			  }
		    }
		  }
		}
		pairs
    }).setName("pairwise_pins_inc").persist(MEMORY_AND_DISK)
    if (debug_mode) {
      logInfo(sdf.format(cal.getTime()) + " : " + pairwise_pins_inc.count().toString())
    }

    val pin_hash_inc = group_pins_inc.flatMap(
      b => {
        b._2.map(s => (s.hashCode().toLong, s))
      }
    ).setName("pin_hash_inc").persist(MEMORY_AND_DISK)
    if (debug_mode) {
      logInfo(sdf.format(cal.getTime()) + " : " + pin_hash_inc.count().toString())
    }
    group_pins_inc.unpersist(blocking=false)
    
    val edges_user = input_edges.map{case (a, b) => (a.hashCode().toLong, b.toLong)}
                                .union(pairwise_pins_inc).setName("edges_user").persist(MEMORY_AND_DISK)
    pairwise_pins_inc.unpersist(blocking=false)
    input_edges.unpersist(blocking = false)
    
    if (debug_mode) {
      logInfo(sdf.format(cal.getTime()) + " : " + edges_user.count().toString())
    }

    val graph = Graph.fromEdgeTuples(edges_user, Long.MaxValue, partitionStrategy)
	edges_user.unpersist(blocking = false)
    
//    val uf = (id: VertexId, data: Long, o: Option[Long]) => {
//      o match {
//        case Some(u) => u
//        case None => id
//      }
//    }
//    val new_graph = graph.outerJoinVertices(edges_user)(uf)
//    if (debug_mode) {
//      new_graph.vertices.map { case (vid, data) => vid + "\t" + data }.saveAsTextFile("/tmp/graph_vertices")
//    }

    val cc = run(graph, maxIterations)
    if (!outFname.isEmpty) {
      val result = cc.vertices.innerJoin(pin_hash_main)(
	    (VertexId, VD, VD2) => (VD2, VD))
      result.map { case (vid, data) => data._1 + "\t" + data._2 }.saveAsTextFile(outFname)
    }
    if (!outIncFname.isEmpty) {
      val result = cc.vertices.innerJoin(pin_hash_inc)(
	    (VertexId, VD, VD2) => (VD2, VD))
      result.map { case (vid, data) => data._1 + "\t" + data._2 }.saveAsTextFile(outIncFname)
    }
    sc.stop()
  }
}