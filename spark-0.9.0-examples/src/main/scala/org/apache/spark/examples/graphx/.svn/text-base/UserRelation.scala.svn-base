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

object UserRelation extends Logging {
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
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
  } // end of connectedComponents
  
  def main(args: Array[String]) {
     if (args.length < 2) {
      System.err.println(
        "Usage: UserRelation <master> <table_input_dir> <name>\n" +
          "    [--output=<output_file>]\n" +
          "        If specified, the file to write the ranks to.\n" +
          "    [--groupFieldId=<group field id>]\n" +
          "        comma separated fields of group id for relate pins, e.g. 1,2,3\n" +
          "    [--pinFieldId=<pin field id>]\n" +
          "        field of jd pin.\n" +
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
    val input_dir = args(1)
    val fname = args(2)
    val options =  args.drop(3).map { arg =>
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
    conf.set("spark.shuffle.use.netty", "true")
    
    var outFname = ""
    var partitionStrategy: Option[PartitionStrategy] = None
    var groupFieldId = ""
    var pinFieldId = -1
    var maxGroupPins = 500
    var debug_mode = false
    var maxIterations = Int.MaxValue

    options.foreach{
      case ("output", v) => outFname = v
      case ("partStrategy", v) => partitionStrategy = Some(pickPartitioner(v))
      case ("groupFieldId", v) => groupFieldId = v
      case ("maxGroupPins", v) => maxGroupPins = v.toInt
      case ("pinFieldId", v) => pinFieldId = v.toInt
      case ("debug_mode", v) => debug_mode = v.toBoolean
      case ("maxIterations", v) => maxIterations = v.toInt
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }
    val sc = new SparkContext(host, "ConnectedComponents(" + fname + ")", conf)
    val input_table = sc.textFile(input_dir)
    val group_pins = input_table.map {
      r => val f = r.split("\\x01|\\s+")
      val key = groupFieldId.split(",").map(id => f(id.trim.toInt)).reduceLeft((res, x) => (res + "," + x))
      (key, f(pinFieldId))
    }.groupByKey().setName("group_pins").persist(MEMORY_AND_DISK)
    val pairwise_pins = group_pins.flatMap(
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
    }).setName("pairwise_pins").persist(MEMORY_AND_DISK)
    var cal = Calendar.getInstance();
    if (debug_mode) {
      logInfo(sdf.format(cal.getTime()) + " : " + group_pins.count().toString())
	}
    val pin_hash = group_pins.flatMap(
      b => {
        b._2.map(s => (s.hashCode().toLong, s))
      }
    ).setName("pin_hash").persist(MEMORY_AND_DISK)
    group_pins.unpersist(blocking=false)
    val graph = Graph.fromEdgeTuples(pairwise_pins, 1, partitionStrategy)
    pairwise_pins.unpersist(blocking=false)
    val cc = run(graph, maxIterations)
    val result = cc.vertices.leftJoin(pin_hash)(
      (VertexId, VD, VD2) => {
        (VD2.getOrElse(""), VD)
      }
    )
    if (!outFname.isEmpty) {
      logWarning("Saving Components of vid to " + outFname)
      result.map{ case (vid, data) => data._1 + "\t" + data._2}.saveAsTextFile(outFname) 
    }
    sc.stop()
  }
}