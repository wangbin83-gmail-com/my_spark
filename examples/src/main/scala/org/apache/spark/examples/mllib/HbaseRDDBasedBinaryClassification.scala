package org.apache.spark.examples.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{SVMWithSGD, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{SquaredL2Updater, L1Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.hadoop.hbase.{HBaseConfiguration}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path;
import collection.JavaConversions._
import Array._
import java.nio.ByteBuffer
import java.lang.{Double => JDouble}
import scala.collection.immutable.TreeMap

/**
 * Created by wywangbin1 on 2014/9/18.
 */
object HbaseRDDBasedBinaryClassification {


  object Algorithm extends Enumeration {
    type Algorithm = Value
    val SVM, LR = Value
  }

  object RegType extends Enumeration {
    type RegType = Value
    val L1, L2 = Value
  }

  import Algorithm._
  import RegType._

  case class Params(
                     table: String = null,
                     hbase_conf: String = null,
                     numIterations: Int = 100,
                     stepSize: Double = 1.0,
                     algorithm: Algorithm = LR,
                     regType: RegType = L2,
                     regParam: Double = 0.1)

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("HbaseRDDBasedBinaryClassification") {
      head("HbaseRDDBasedBinaryClassification: an example app for binary classification.")
      opt[Int]("numIterations")
        .text("number of iterations")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("stepSize")
        .text("initial step size (ignored by logistic regression), " +
        s"default: ${defaultParams.stepSize}")
        .action((x, c) => c.copy(stepSize = x))
      opt[String]("algorithm")
        .text(s"algorithm (${Algorithm.values.mkString(",")}), " +
        s"default: ${defaultParams.algorithm}")
        .action((x, c) => c.copy(algorithm = Algorithm.withName(x)))
      opt[String]("regType")
        .text(s"regularization type (${RegType.values.mkString(",")}), " +
        s"default: ${defaultParams.regType}")
        .action((x, c) => c.copy(regType = RegType.withName(x)))
      opt[Double]("regParam")
        .text(s"regularization parameter, default: ${defaultParams.regParam}")
      opt[String]("hbase_conf")
        .text(s"hbase conf type, default: /etc/hbase/conf/hbase-site.xml")
        .action((x, c) => c.copy(hbase_conf = x))
      arg[String]("<table>")
        .required()
        .text("hbase table")
        .action((x, c) => c.copy(table = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit
          |  --class org.apache.spark.examples.mllib.HbaseRDDBasedBinaryClassification \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --algorithm LR --regType L2 --regParam 1.0 \
          |  --hbase_conf /etc/hbase/conf/hbase-site.xml \
          |  table_name
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"HbaseRDDBasedBinaryClassification with $params")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    val hbase_conf = HBaseConfiguration.create()
    // Add local HBase conf
    hbase_conf.addResource(new Path(params.hbase_conf))
    hbase_conf.set(TableInputFormat.INPUT_TABLE, params.table)
    val hBaseRDD = sc.newAPIHadoopRDD(hbase_conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    object ArrayByteOrdering extends Ordering[Array[Byte]] {
      def compare(a: Array[Byte], b: Array[Byte]): Int = {
        if (a eq null) {
          if (b eq null) 0
          else -1
        }
        else if (b eq null) 1
        else {
          val L = math.min(a.length, b.length)
          var i = 0
          while (i < L) {
            if (a(i) < b(i)) return -1
            else if (b(i) < a(i)) return 1
            i += 1
          }
          if (L < b.length) -1
          else if (L < a.length) 1
          else 0
        }
      }
    }
    val examples = hBaseRDD.map{case(key, value) =>
      val values = value.getFamilyMap("person_risk".getBytes())
      val label = new String(values.get("isrisk".getBytes())).toInt
      val size = values.size() - 1
      val indices = range(0, size)
      val features = new Array[Double](size)
      var i: Int = 0
      TreeMap(values.toSeq:_*)(ArrayByteOrdering).map{case(k, v) =>
        val kStr = new String(k)
        if (kStr != "isrisk") {
          try {
            features.update(i, JDouble.parseDouble(new String(v).trim()).toInt)
            i = i + 1
          } catch {
            case ex: Throwable => println("bad value: " + new String(v) + ex.printStackTrace())
            throw ex
          }
        }}
      LabeledPoint(label, Vectors.sparse(size, indices, features))
    }.cache()
    
    val splits = examples.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    examples.unpersist(blocking = false)

    val updater = params.regType match {
      case L1 => new L1Updater()
      case L2 => new SquaredL2Updater()
    }

    val model = params.algorithm match {
      case LR =>
        val algorithm = new LogisticRegressionWithLBFGS()
        algorithm.optimizer
          .setNumIterations(params.numIterations)
          .setUpdater(updater)
          .setRegParam(params.regParam)
        algorithm.run(training).clearThreshold()
      case SVM =>
        val algorithm = new SVMWithSGD()
        algorithm.optimizer
          .setNumIterations(params.numIterations)
          .setStepSize(params.stepSize)
          .setUpdater(updater)
          .setRegParam(params.regParam)
        algorithm.run(training).clearThreshold()
    }
    println(s"model weight: ${model.weights}")
    println(s"model intercept: ${model.intercept}")
    
    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    val metrics = new BinaryClassificationMetrics(predictionAndLabel)

    println(s"Test areaUnderPR = ${metrics.areaUnderPR()}.")
    println(s"Test areaUnderROC = ${metrics.areaUnderROC()}.")

    sc.stop()
  }
}
