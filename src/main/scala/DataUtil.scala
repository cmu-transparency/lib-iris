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

object DataUtil extends App {
  case class CmdLine(
    input_data: Path = null,
    input_schema: Path = null,
    input_delimiter: String = ",",
    output_data: Path = null,
    output_schema: Path = null,
    action: String = "convert"
  )

  val parser = new scopt.OptionParser[CmdLine](programName = "DataUtil") {
    head("DataUtil", "0.1")

    val outopts = { () =>
      opt[String]("out")
        .text("Output data file.")
        .required
        .action((x, c) => c.copy(output_data = new Path(x)))
      opt[String]("out-schema")
        .text("Output data schema.")
        .optional
        .action((x, c) => c.copy(input_schema = new Path(x)))
    }

    opt[String]("in")
      .text("Input data file.")
      .required
      .action((x, c) => c.copy(input_data = new Path(x)))
    opt[String]("in-schema")
      .text("Input data schema.")
      .optional
      .action((x, c) => c.copy(input_schema = new Path(x)))
    opt[String]("in-delim")
      .text("Input delimiter.")
      .optional
      .action((x, c) => c.copy(input_delimiter = x))

    cmd("convert")
      .text("Convert datafile.")
      .required
      .action((x, c) => c.copy(action = "convert"))
      .children(outopts())
  }

  val actions = Map[String,(CmdLine => Unit)](
    "convert" -> ActionConvert
  )

  parser.parse(args, CmdLine()) match {
    case Some(cmdline) => actions(cmdline.action)(cmdline)
    case None => ()
  }

  def ActionConvert(cmdline: CmdLine): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val input_data = cmdline.input_data
    val output_data = cmdline.output_data

    println(s"convert $input_data to $output_data")

    FSUtils.deleteIfExists(output_data)

    val data = SparkUtil.csvReader
      .option("delimiter", cmdline.input_delimiter)
      .option("inferSchema", "true")
      .load(cmdline.input_data.toString)
      //.persist(StorageLevel.MEMORY_ONLY)

    println(s"input schema")
    data.schema.printTreeString

    val dataConverted = convertColumnNamesParquet(data)
    println(s"output schema")
    dataConverted.schema.printTreeString

    dataConverted.write.parquet(cmdline.output_data.toString)
  }

  def convertColumnNameParquet(col: String): String = {
    // parquet restricts " ,;{}()\n\t="

    col
      .replace(' ', '_')
      .replace('(', '[')
      .replace(')', ']')
  }

  def convertColumnNamesParquet(data: DataFrame): DataFrame = {
    data.columns.foldLeft(data){case (data, col) =>
      data.withColumnRenamed(col, convertColumnNameParquet(col))
    }
  }
}
