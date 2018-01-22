package edu.cmu.spf.iris

import scala.collection.JavaConversions._

import org.apache.log4j.{ Logger, Level }

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SQLContext }

import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}

import org.apache.spark.ml.param._

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.sql.types._

import org.apache.spark.ml.{Pipeline, PipelineStage, Transformer}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

import org.apache.spark.storage.StorageLevel

import java.io.{File,FileWriter,BufferedWriter}
import org.apache.commons.io.FileUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path

object DataStats extends App {
  case class CmdLine(
    input_data: Path = null,
    input_schema: Path = null,
    action: String = "stats"
  )

  val parser = new scopt.OptionParser[CmdLine](programName = "DataStats") {
    head("DataStats", "0.1")

    opt[String]("in")
      .text("Input data file.")
      .required
      .action((x, c) => c.copy(input_data = new Path(x)))
    opt[String]("in-schema")
      .text("Input data schema.")
      .optional
      .action((x, c) => c.copy(input_schema = new Path(x)))

    cmd("stats")
      .text("Per-feature statistics.")
      .required
      .action((x, c) => c.copy(action = "stats"))
      //.children(outopts())
  }

  val actions = Map[String,(CmdLine => Unit)](
    "stats" -> ActionStats
  )

  parser.parse(args, CmdLine()) match {
    case Some(cmdline) => actions(cmdline.action)(cmdline)
    case None => ()
  }

  def ActionStats(cmdline: CmdLine): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val input_data = cmdline.input_data

    println(s"stats about $input_data")

    val data = SparkUtil.csvReader
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(cmdline.input_data.toString)
      //.persist(StorageLevel.MEMORY_ONLY)

    println(s"input schema")
    data.schema.printTreeString

    

  }
}
