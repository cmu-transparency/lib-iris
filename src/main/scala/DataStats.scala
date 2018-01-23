package edu.cmu.spf.iris

import scala.collection.JavaConversions._

import org.apache.spark.sql.functions._

import scala.util.matching.Regex

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

import java.io.{File,PrintWriter,FileWriter,BufferedWriter}
import org.apache.commons.io.FileUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path

object DataStats extends App {
  case class CmdLine(
    input_data: Path = null,
    input_schema: Path = null,
    dot_output: Path = null,
    dot_weight: String = "corr",
    columns: Regex = ".*".r,
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
    opt[String]("cols")
      .text("Select columns by regexp")
      .optional
      .action((x, c) => c.copy(columns = x.r))

    cmd("stats")
      .text("Per-feature statistics.")
      .required
      .action((x, c) => c.copy(action = "stats"))

    val pairstat_opts = { () =>
      opt[String]("dot_out")
        .text("Output dot file.")
        .optional
        .action((x, c) => c.copy(dot_output = new Path(x)))
      opt[String]("dot_weight")
        .text("Statistic to use for edge weights in dot output.")
        .optional
        .action((x, c) => c.copy(dot_weight = x))
    }

    cmd("pairstats")
      .text("Per-feature-pair statistics.")
      .required
      .action((x, c) => c.copy(action = "pairstats"))
      .children(pairstat_opts())
  }

  val actions = Map[String,(CmdLine => Unit)](
    "stats" -> ActionStats,
    "pairstats" -> ActionPairStats
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

    val data_with_info = data.columns.zip(data.schema.fields).filter {
      case (cname, _) => cname match {
        case cmdline.columns() => true
        case _ => false
      }
    }

    data_with_info.foreach { case (col, info) =>
      val typ = info.dataType
      println(s"$col: $typ")
      data.describe(col).collect.foreach { stat =>
        println("  " + stat.mkString(": "))
      }

      import org.apache.spark.ml.regression.LinearRegression
      import org.apache.spark.ml.evaluation.RegressionEvaluator
      import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}

      val evaluator = new RegressionEvaluator()
        .setLabelCol(col)
        .setPredictionCol("prediction")
        .setMetricName("rmse")

      val assemble = new VectorAssembler()
        .setOutputCol("features")
        .setInputCols(data_with_info.map(_._1).filter{_ != col})

      val dat = assemble.transform(data)

//      dat.show(5)

      val lr = new LinearRegression()//new RandomForestRegressor() 
        .setLabelCol(col)
//        .setMaxIter(100)
//        .setRegParam(0.0)
      val model = lr.fit(dat)
      //val rmse: Double = model.summary.rootMeanSquaredError
      val rmse = evaluator.evaluate(model.transform(dat))

      val temp = data.select(stddev(col))
      val stddev_val: Double = temp.collect().head.getDouble(0)

      println(f"""  rmse of linear: $rmse%3.3f/$stddev_val%3.3f""")
    }
  }

  def ActionPairStats(cmdline: CmdLine): Unit = {
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

    val stat = data.stat

    val data_with_info = data.columns.zip(data.schema.fields).filter {
      case (cname, _) => cname match {
        case cmdline.columns() => true
        case _ => false
      }
    }

    import org.apache.spark.ml.regression.LinearRegression
    //import org.apache.spark.ml.regression.RandomForestRegressor

    val assemble = new VectorAssembler()
      .setOutputCol("features")

    val stats = Seq(
      ("Pearson correlation", "corr",
        DataTypes.StringType != _,
        (col1: String, col2: String) => {
          val corr = stat.corr(col1, col2)
          (f"$corr%3.3f", corr, Math.abs(corr))
        }),
      ("covariance", "cov",
        DataTypes.StringType != _,
        (col1: String, col2: String) => {
          val cov = stat.cov(col1, col2)
          (f"$cov%3.3f", cov, Math.abs(cov))
        }),
      ("linear", "linear",
        DataTypes.StringType != _,
        (col1: String, col2: String) => {
          assemble.setInputCols(Seq(col1).toArray)
          val dat = assemble.transform(data)
          val lr = new LinearRegression().setLabelCol(col2)
          val model = lr.fit(dat)
          val rmse: Double = model.summary.rootMeanSquaredError
          val temp = data.select(stddev(col2))
          val stddev_val: Double = temp.collect().head.getDouble(0)
          (f"""$rmse%3.3f/$stddev_val%3.3f""", rmse, 1 - rmse/stddev_val)
        })
    )

    val (writer, close_writer) = {
      if (null != cmdline.dot_output) {
        val temp = new PrintWriter(cmdline.dot_output.toString)
        ((st: String) => {
          temp.write(st)
        },() => {
          temp.close()
        })
      } else {
        ((_: String) => {}, () => {})
      }
    }

    writer("digraph G {\n")
    //writer("graph G {\n")
//    writer("  node [shape=circle];\n")

    val dotify_name = { s: String => s
      .replace("[", "")
      .replace("]", "")
    }

    data_with_info.foreach { case (col1, info1) =>
      val col1_name = dotify_name(col1)
      val typ1 = info1.dataType

      data_with_info.foreach { case (col2, info2) =>
        val col2_name = dotify_name(col2)
        val typ2 = info2.dataType
        println(s"$col1 [$typ1], $col2 [$typ2]")
        stats.foreach { case (stat_name, stat_short_name, stat_type_filter, stat_fun) =>
          if (stat_type_filter(typ1) && stat_type_filter(typ2)) {
            val (stat_label, stat_val, stat_weight) = stat_fun(col1, col2)
            println(s"  $stat_name: $stat_val")
            if (cmdline.dot_weight == stat_short_name) {

              val len_string = f"${5 * (1 - stat_weight)}%6.6f"
              val weight_string = f"$stat_weight%6.6f"
              val width_string = f"${5 * stat_weight}%6.6f"
              val color_string = f"0.000 0.000 ${1-stat_weight}%6.6f"

              writer(s"""  $col1_name -> $col2_name [fillcolor="#ffffff44", fontcolor="$color_string", color="$color_string", len=$len_string, penwidth=$width_string, label="$stat_label"];\n""")
              //writer(s"""  $col1_name -- $col2_name [weight=$weight_string, label="$label_string"];\n""")
              //writer(s"""  $col1_name -- $col2_name [label="$label_string"];\n""")
            }
          }
        }
      }
    }

    writer(s"}\n")
    close_writer()

  }
}
