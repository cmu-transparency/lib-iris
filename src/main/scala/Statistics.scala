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

import java.io.{ File, FileWriter, BufferedWriter }

object Statistics extends App {
  case class CmdLine(
    input_file: File = new File("."),
    discrete_bins: Int = 10)

  val parser = new scopt.OptionParser[CmdLine]("Statistics") {
    head("Statistics", "0.1")
    opt[File]('i', "in")
      .required
      .action((x, c) => c.copy(input_file = x))
    opt[Int]('b', "bins")
      .action((x, c) => c.copy(discrete_bins = x))
  }
  parser.parse(args, CmdLine()) match {
    case Some(cmdline) => RunStatistics(cmdline)
    case None => ()
  }

  def RunStatistics(cmdline: CmdLine): Unit = {
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
      StructField("Application Date Indicator", StringType, true)
    ))

    val data = SparkUtil.csvReader
      .option("delimiter", "\t")
      .option("inferSchema", "false")
      .schema(schema)
      .load(cmdline.input_file.toString)
      .persist(StorageLevel.MEMORY_ONLY)

    val quantizer = new QuantileDiscretizer()
      .setNumBuckets(cmdline.discrete_bins)
      .setHandleInvalid("keep")

    data.columns
      .zip(data.schema.fields)
      .foreach { case (column_name, f) =>
        var buckets: Option[Array[Double]] = None

          val column_type = f.dataType
          println(s"$column_name ($column_type)")

          val column = {
            column_type match {
              case StringType | IntegerType => data
              case DoubleType =>
                val bucketizer = quantizer
                  .setInputCol(column_name)
                  .setOutputCol(column_name + "_discrete")
                  .fit(data)

                buckets = Some(bucketizer.getSplits)

                bucketizer.transform(data)
                  .drop(column_name)
                  .withColumnRenamed(column_name + "_discrete", column_name)
            }
          }.select(column_name)

        val values = column
          .groupBy(column_name)
          .count
          .collect

        buckets match {
          case None => values.foreach {case row =>
            println(s"  ${row.get(0)}: ${row.get(1)}")
          }
          case Some(splits) => values.foreach {case row =>
            val label = if (row.isNullAt(0)) { "null" } else {
              val quantile: Int = row.getDouble(0).toInt
              "["+splits(quantile).toString + "," + splits(quantile+1).toString + ")"
            }
            println(s"  ${row.get(0)} $label: ${row.get(1)}")
          }
        }
        }
  }
  }
