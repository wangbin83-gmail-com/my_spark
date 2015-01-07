package org.apache.spark.examples

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.fs.Path;
import scopt.OptionParser

/**
 * Created by wywangbin1 on 2014/9/30.
 */
object HbaseRDDReadTest {
  case class Params(
                     table: String = null,
                     hbase_conf: String = null)

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("HbaseRDDReadTest") {
      head("HbaseRDDReadTest")
      opt[String]("hbase_conf")
        .text(s"hbase conf type, default: /etc/hbase/conf/hbase-site.xml")
        .action((x, c) => c.copy(hbase_conf = x))
      arg[String]("<table>")
        .required()
        .text("hbase table")
        .action((x, c) => c.copy(table = x))
      note(
        """
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
  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName(s"HbaseRDDTest")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    val hbase_conf = HBaseConfiguration.create()
    // Add local HBase conf
    hbase_conf.addResource(new Path(params.hbase_conf))
    hbase_conf.set(TableInputFormat.INPUT_TABLE, params.table)
    val hBaseRDD = sc.newAPIHadoopRDD(hbase_conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hBaseRDD.cache();
    hBaseRDD.unpersist(true)
    sc.stop()
  }
}
