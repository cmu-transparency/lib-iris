package edu.cmu.spf.iris

import scala.collection.JavaConversions._

import org.apache.log4j.{ Logger, Level }

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SQLContext }

import org.apache.spark.sql.{ SparkSession, DataFrame }

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.sql.types._

object SparkUtil {
  Logger.getLogger("Remoting").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("spark").setLevel(Level.ERROR)
  Logger.getLogger("spark").setLevel(Level.ERROR)
  Logger.getLogger("databricks").setLevel(Level.ERROR)
  Logger.getRootLogger().setLevel(Level.ERROR)

  val sql = SparkSession
    .builder
    .master("local[*]")
    .config("spark.ui.showConsoleProgress", false)
    .config("spark.driver.memory", "16g")
    .config("spark.executor.memory", "64g")
    .appName("SparkUtil")
    .getOrCreate()

  val csvReader = sql.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "FAILFAST")
    .option("nullValue", "?")

  def csvWrite(
    dataframe: DataFrame,
    file: String,
    delimiter: String = ","): Unit = {
    dataframe.coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", delimiter)
      .save(file)
  }

  /* this doesn't work, there is no SetType, only ArrayType */
  class TokenizerSet(val sep: String = ",", override val uid: String)
    extends UnaryTransformer[String, Set[String], TokenizerSet] {

    override protected def outputDataType: DataType =
      new ArrayType(StringType, true)

    override protected def createTransformFunc: String => Set[String] = {
      originStr => sep.split(originStr).toSet
    }
  }

}
