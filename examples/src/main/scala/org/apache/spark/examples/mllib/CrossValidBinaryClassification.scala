package org.apache.spark.examples.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{SVMWithSGD, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.optimization.{SquaredL2Updater, L1Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

/**
 * Created by wywangbin1 on 2014/9/12.
 */
object CrossValidBinaryClassification {

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
                     input: String = null,
                     numIterations: Int = 100,
                     stepSize: Double = 1.0,
                     algorithm: Algorithm = LR,
                     regType: RegType = L2,
                     regParam: Double = 0.1)

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("CrossValidBinaryClassification") {
      head("CrossValidBinaryClassification: an example app for binary classification.")
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
      arg[String]("<input>")
        .required()
        .text("input paths to labeled examples in LIBSVM format")
        .action((x, c) => c.copy(input = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit
          |  --class org.apache.spark.examples.mllib.CrossValidBinaryClassification \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --algorithm LR --regType L2 --regParam 1.0 \
          |  data/mllib/sample_binary_classification_data.txt
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"CrossValidBinaryClassification with $params")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val examples = MLUtils.loadLibSVMFile(sc, params.input).cache()

    val splits = examples.randomSplit(Array(0.8, 0.2))
    val main_training = splits(0).cache()
    val main_test = splits(1).cache()

    val bad = examples.filter(r => r.label == 1)
    val good = examples.filter(r => r.label == 0)
    val bad_splits = bad.randomSplit(Array.fill[Double](10)(0.1))
    val good_splits = good.randomSplit(Array.fill[Double](10)(0.1))

    println("bad label split:")
    bad_splits.foreach((rdd) => println(rdd.count()))
    println("\ngood label split:")
    good_splits.foreach((rdd) => println(rdd.count()))

    val updater = params.regType match {
      case L1 => new L1Updater()
      case L2 => new SquaredL2Updater()
    }

    def split_train(i: Int) = {
      val bad_training = sc.union(
        bad_splits.zipWithIndex.filter{case(s, j) => i != j}.map{case(s, j) => s})
      val good_training =  sc.union(
        good_splits.zipWithIndex.filter{case(s, j) => i != j}.map{case(s, j) => s})
      val training = sc.union(bad_training, good_training).cache()
      val test = sc.union(bad_splits(i), good_splits(i))
      train(training, test)
    }

    def train(training: RDD[LabeledPoint], test: RDD[LabeledPoint]) = {
      val numTraining = training.count()
      val numTest = test.count()
      println(s"Training: $numTraining, test: $numTest.")
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
    }
    println("test all:")
    train(main_training, main_test)
    println("test split")
    for(i <- 0 to 9) {split_train(i)}
    examples.unpersist(blocking = false)
    sc.stop()
  }
}
