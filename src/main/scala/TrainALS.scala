package edu.cmu.spf.iris

import scala.collection.JavaConversions._

import scala.util.matching.Regex


import org.apache.spark.sql.Row

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

import org.apache.spark.ml.recommendation.ALS
//import org.apache.spark.ml.recommendation.MatrixFactorizationModel
import org.apache.spark.ml.recommendation.ALS.Rating

object TrainALS extends App {
  case class CmdLine(
    input_data: Path = null,
    input_schema: Path = null,
    action: String = "train",
    output_user_latents: Path = null,
    output_item_latents: Path = null
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

    opt[String]("out-users")
      .text("Output data file for user latents.")
      .optional
      .action((x, c) => c.copy(output_user_latents = new Path(x)))

    opt[String]("out-items")
      .text("Output data file for item latents.")
      .optional
      .action((x, c) => c.copy(output_item_latents = new Path(x)))

    cmd("train")
      .text("Per-feature statistics.")
      .required
      .action((x, c) => c.copy(action = "train"))

  }

  val actions = Map[String,(CmdLine => Unit)](
    "train" -> ActionTrain
  )

  parser.parse(args, CmdLine()) match {
    case Some(cmdline) => actions(cmdline.action)(cmdline)
    case None => ()
  }

  def ActionTrain(cmdline: CmdLine): Unit = {
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

    import SparkUtil.sql.implicits._

    val ratings = data.rdd.map { row =>
      Rating(
        row.getInt(0),
        row.getInt(1),
        row.getInt(2)
      )
    }

    val als = new ALS()
      .setItemCol("movieId")
      .setUserCol("userId")
      .setRatingCol("rating")
      .setMaxIter(100)
      .setCheckpointInterval(2)

    val model = als.fit(data)

    import SparkUtil.sql.implicits._

    if (null != cmdline.output_item_latents) {
      println(s"writing item latents to ${cmdline.output_item_latents}")

      FSUtils.deleteIfExists(cmdline.output_item_latents)

      val flat = model.itemFactors.selectExpr(
        Seq("id") ++
          Range(0, als.getRank).map{"features[" + _ + "]"}.toSeq : _*
      )

      flat.coalesce(1).write.format("csv")
        .option("header", true)
        .save(cmdline.output_item_latents.toString)
    }

    if (null != cmdline.output_user_latents) {
      println(s"writing user latents to ${cmdline.output_user_latents}")

      FSUtils.deleteIfExists(cmdline.output_user_latents)

      val flat = model.userFactors.selectExpr(
        Seq("id") ++
          Range(0, als.getRank).map{"features[" + _ + "]"}.toSeq : _*
      )

      flat.coalesce(1).write.format("csv")
        .option("header", true)
        .save(cmdline.output_user_latents.toString)
    }
  }
}
