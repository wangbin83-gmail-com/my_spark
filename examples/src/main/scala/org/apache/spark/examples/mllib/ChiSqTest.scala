/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

/**
 * Created by wywangbin1 on 2014/9/5.
 */
object ChiSqTest {
  case class Params(input: String = null)

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("ChiSqTest") {
      head("ChiSqTest: to the independence test.")
      arg[String]("<input>")
        .required()
        .text("input paths to labeled examples in LIBSVM format")
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName(s"ChiSqTest with $params")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    val examples = MLUtils.loadLibSVMFile(sc, params.input).cache()
    val chi = Statistics.chiSqTest(examples)
    for (feature <- chi) {
      println(feature.toString)
    }
  }
}
