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
    input_data_more: List[Path] = List(),
    input_schema: Path = null,
    input_delimiter: String = null,
    output_format: String = "csv",
    output_data: Path = null,
    output_schema: Path = null,
    output_delimiter: String = ",",
    action: String = "convert",
    sql_command: String = null
  )

  val parser = new scopt.OptionParser[CmdLine](programName = "DataUtil") {
    head("DataUtil", "0.1")

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
      .action((x, c) => c.copy(action = "convert"))
      .children(
        opt[String]("out")
          .text("Output data file.")
          .required
          .action((x, c) => c.copy(output_data = new Path(x))),
        opt[String]("out-schema")
          .text("Output data schema.")
          .optional
          .action((x, c) => c.copy(input_schema = new Path(x))),
        opt[String]("out-delim")
          .text("Output delimiter.")
          .optional
          .action((x, c) => c.copy(output_delimiter = x)),
        opt[String]("out-format")
          .text("Output format [csv, parquet, tsv].")
          .optional
          .action((x, c) => c.copy(output_delimiter = x))
      )

    cmd("sql")
      .text("Select data as specified by an SQL statement.")
      .action((x, c) => c.copy(action = "sql"))
      .children(
        opt[String]("sql")
          .text("SQL statement.")
          .required
          .action((x, c) => c.copy(sql_command = x)),
        opt[String]("in-more")
          .text("Additional input data files, tables will be named inputN where N is the Nth in-more option's input.")
          .optional
          .unbounded
          .action((x, c) => c.copy(input_data_more = c.input_data_more :+ new Path(x)))
      )
  }

  val actions = Map[String,(CmdLine => Unit)](
    "convert" -> ActionConvert,
    "sql" -> ActionSQL
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

    val delim = if (null != cmdline.input_delimiter) {
      cmdline.input_delimiter
    } else {
      DUtils.guessDelimeter(new File(input_data.toString))
    }

    val data = SparkUtil.csvReader
      .option("delimiter", delim)
      .option("inferSchema", "true")
      .load(cmdline.input_data.toString)
      //.persist(StorageLevel.MEMORY_ONLY)

    println(s"input schema")
    data.schema.printTreeString

    if (cmdline.output_format == "parquet") {
      val dataConverted = convertColumnNamesParquet(data)
      println(s"output schema")
      dataConverted.schema.printTreeString

      dataConverted.write.parquet(cmdline.output_data.toString)

    } else {
      data.coalesce(1).write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", cmdline.output_delimiter)
        .option("quote", "\"")
        .save(cmdline.output_data.toString)
    }
  }

  def ActionSQL(cmdline: CmdLine): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val input_data = cmdline.input_data
    val output_data = cmdline.output_data
    val sql_command = cmdline.sql_command
    val input_data_more = cmdline.input_data_more

    println(s"select [$sql_command] from $input_data to $output_data")

    FSUtils.deleteIfExists(output_data)

    val delim = if (null != cmdline.input_delimiter) {
      cmdline.input_delimiter
    } else {
      DUtils.guessDelimeter(new File(input_data.toString))
    }

    println(s"table [input] is from [$input_data]")
    val data = SparkUtil.csvReader
      .option("delimiter", delim)
      .option("inferSchema", "true")
      .option("quote", "\u0000") // jobs.tsv does not use quotes but
                                 // has a string that starts with a
                                 // quote which messes things up
                                 // without this option.
      .load(cmdline.input_data.toString)
      //.persist(StorageLevel.MEMORY_ONLY)


    data.createOrReplaceTempView("input")

    input_data_more.zip(1 to input_data_more.length + 1).foreach { case (f, i) =>
      println(s"table [input$i] is from [$f]")

      val dataTemp = SparkUtil.csvReader
      .option("delimiter", delim)
      .option("inferSchema", "true")
      .option("quote", "\u0000") // jobs.tsv does not use quotes but
                                 // has a string that starts with a
                                 // quote which messes things up
                                 // without this option.
      .load(f.toString)

      dataTemp.createOrReplaceTempView(s"input$i")
    }

    println(s"input schema")
    data.schema.printTreeString

    val dataSelected = SparkUtil.sql.sql(sql_command).coalesce(1)

    println(s"output schema")
    dataSelected.schema.printTreeString

    dataSelected.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", delim)
      .save(cmdline.output_data.toString)
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
