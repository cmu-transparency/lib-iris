package edu.cmu.spf.iris

import scala.collection.JavaConversions._

import org.apache.log4j.{ Logger, Level }

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SQLContext }

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.sql.types._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.classification.RandomForestClassifier

import java.io.{ File, FileWriter, BufferedWriter }

object BenchForest extends App {
  case class CmdLine(
    input_file: File = null,
    discrete_bins: Int = 10,
    target: String = null)

  val parser = new scopt.OptionParser[CmdLine]("Statistics") {
    head("Statistics", "0.1")
    opt[File]('i', "in")
      .required
      .action((x, c) => c.copy(input_file = x))
    opt[Int]('b', "bins")
      .action((x, c) => c.copy(discrete_bins = x))
    opt[String]('t', "target")
      .action((x, c) => c.copy(target = x))
  }
  parser.parse(args, CmdLine()) match {
    case Some(cmdline) => RunForest(cmdline)
    case None => ()
  }

  def RunForest(cmdline: CmdLine): Unit = {
    val schema = StructType(Array(
      StructField("As of Year", StringType, true),
      StructField("Respondent ID", DoubleType, true),
      StructField("Agency Code", StringType, true),
      StructField("Loan Type", StringType, true),
      StructField("Property Type", StringType, true),
      StructField("Loan Purpose", StringType, true),
      StructField("Occupancy", StringType, true),
      StructField("Loan Amount (000s)", DoubleType, true),
      StructField("Preapproval", StringType, true),
      StructField("Action Type", StringType, true),
      StructField("MSA/MD", DoubleType, true),
      StructField("State Code", StringType, true),
      StructField("County Code", StringType, true),
      StructField("Census Tract Number", DoubleType, true),
      StructField("Applicant Ethnicity", StringType, true),
      StructField("Co Applicant Ethnicity", StringType, true),
      StructField("Applicant Race 1", StringType, true),
      StructField("Applicant Race 2", StringType, true),
      StructField("Applicant Race 3", StringType, true),
      StructField("Applicant Race 4", StringType, true),
      StructField("Applicant Race 5", StringType, true),
      StructField("Co Applicant Race 1", StringType, true),
      StructField("Co Applicant Race 2", StringType, true),
      StructField("Co Applicant Race 3", StringType, true),
      StructField("Co Applicant Race 4", StringType, true),
      StructField("Co Applicant Race 5", StringType, true),
      StructField("Applicant Sex", StringType, true),
      StructField("Co Applicant Sex Applicant", StringType, true),
      StructField("Income (000s)", DoubleType, true),
      StructField("Purchaser Type", StringType, true),
      StructField("Denial Reason 1", StringType, true),
      StructField("Denial Reason 2", StringType, true),
      StructField("Denial Reason 3", StringType, true),
      StructField("Rate Spread", DoubleType, true),
      StructField("HOEPA Status", StringType, true),
      StructField("Lien Status", StringType, true),
      StructField("Edit Status", StringType, true),
      StructField("Sequence Number", DoubleType, true),
      StructField("Population", DoubleType, true),
      StructField("Minority Population %", DoubleType, true),
      StructField("FFIEC Median Family Income", DoubleType, true),
      StructField("Tract to MSA/MD Income %", DoubleType, true),
      StructField("Number of Owner-occupied units", DoubleType, true),
      StructField("Number of 1-to 4-Family units", DoubleType, true),
      StructField("Application Date Indicator", StringType, true)))

    println(s"loading ${cmdline.input_file}")

    val data = SparkUtil.csvReader
      .option("delimiter", "\t")
      .option("inferSchema", "false")
      .schema(schema)
      .load(cmdline.input_file.toString)
      .persist(StorageLevel.MEMORY_ONLY)

    val target_column =
      if (null != cmdline.target) { cmdline.target } else { data.columns.last }

    println(s"transforming features")

    val data_feats = SparkUtil.buildFeatures(
      data,
      input_columns = data.columns.toSet,
      output_column = "features",
      exclude_in_vector = Set[String](target_column)
    ).persist(StorageLevel.MEMORY_ONLY)

    //println(data_feats)
    //data_feats.collect.foreach { row => println(" " + row) }
    //println("count = " + data_feats.count)
    //data_feats.schema.printTreeString

    val classer = new RandomForestClassifier()
      .setLabelCol(target_column)
      .setFeaturesCol("features")
      .setNumTrees(10)
      .setMaxDepth(5)

    println("training")

    val model = classer.fit(data_feats)
    println(model.toDebugString)

  }
}
