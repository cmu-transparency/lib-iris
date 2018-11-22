package edu.cmu.spf.iris

import scala.collection.JavaConversions._

import org.apache.log4j.{ Logger, Level }

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SQLContext }

import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}

//import org.apache.spark.ml.param._
import org.apache.spark.ml.param._

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.sql.types._

import org.apache.spark.ml.{Pipeline, PipelineStage, Transformer}
import org.apache.spark.ml.feature.{
  StringIndexer, VectorAssembler, QuantileDiscretizer
}

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
    .config("spark.executor.memory", "128g")
    .config("spark.sql.crossJoin.enabled", "true")
    .appName("SparkUtil")
    .getOrCreate()

  sql.sparkContext.setCheckpointDir("checkpoint/")

  val csvReader = sql.read
    .format("csv")//"com.databricks.spark.csv")
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
      .format("csv")//"com.databricks.spark.csv")
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

  /*
  class Renamer[IN](override val uid: String)
      extends UnaryTransformer[IN,IN,Renamer[IN]]
      with DefaultParamsWritable {

    def this() = this(Identifiable.randomUID("Renamer"))

//    override def setInputCol(value: String): T = set(inputCol, value).asInstanceOf[T]
//    override def setOutputCol(value: String): T = set(outputCol, value).asInstanceOf[T]

    var out_type: DataType = null

    override def transform(df: Dataset[_]): DataFrame = {
      out_type = df.schema(getInputCol).dataType
      df.withColumnRenamed(getInputCol, getOutputCol)
   }

    def createTransformFunc: IN => IN = {
      in => in
    }
    def outputDataType: DataType = out_type
  }*/

  class Renamer(
    val inputCol: String,
    val outputCol: String,
    val uid: String = Identifiable.randomUID("Renamer"))
      extends Transformer {

    def transformSchema(schema: StructType): StructType = {
      //println(s"transformSchema: rename column $inputCol to $outputCol")
      val inputType = schema(inputCol).dataType
      val nullable  = schema(inputCol).nullable
        if (schema.fieldNames.contains(outputCol)) {
          throw new IllegalArgumentException(s"Output column ${outputCol} already exists.")
        }
        val outputFields = schema.fields :+
        StructField(outputCol, inputType, nullable = nullable)
        StructType(outputFields)
    }

    def transform(df: Dataset[_]): DataFrame = {
      //println(s"transform: rename column $inputCol to $outputCol")
      df.withColumnRenamed(inputCol, outputCol)
   }

    def copy(extra: ParamMap): Renamer = {
      new Renamer(inputCol, outputCol, uid)
    }
  }

    class Dropper(
    val inputCol: String,
    val uid: String = Identifiable.randomUID("Dropper"))
      extends Transformer {

      def transformSchema(schema: StructType): StructType = {
        //println(s"transformSchema: delete column $inputCol")
        if (! schema.fieldNames.contains(inputCol)) {
          throw new IllegalArgumentException(s"Input column ${inputCol} does not exists.")
        }
        StructType(schema.fields.filter{f => f.name != inputCol})
    }

      def transform(df: Dataset[_]): DataFrame = {
        //println(s"transform: delete column $inputCol")
        df.drop(inputCol)
      }

    def copy(extra: ParamMap): Dropper = {
      new Dropper(inputCol, uid)
    }
  }

  def buildFeatures(
    _data: DataFrame,
    input_columns: Set[String],
    output_column: String="features",
    exclude_in_vector: Set[String] = Set[String]()
  ): DataFrame = {

    println("replacing missing values")
    val data = _data
      .na.fill("")
      .na.fill(0.0)
      .na.fill(0).persist

    val set_columns = input_columns.toSet

    val schema = data.schema

    val stages = data.columns.zip(data.schema.fields)
      .filter{case (col_name, _) => set_columns.contains(col_name)}
      .flatMap {
      case (col_name, col_info) =>
        //println(s"${col_name} ${col_info.dataType} ${col_info.metadata}")

          val new_name = "_" + col_name
          col_info.dataType match {
            case StringType => List(new StringIndexer()
                .setInputCol(col_name)
                .setOutputCol(new_name),
              new Dropper(col_name),
              new Renamer(new_name, col_name)
            )
            case DoubleType | IntegerType => List()
              //val temp1 = new Renamer(col_name, new_name)
              //List(temp)
          }
    }

    val pipeline = new Pipeline().setStages(stages)
    println("fitting transformers")
    val trans = pipeline.fit(data)
    println("running transformers")

    val tempData = trans.transform(data)

    val newSchema = StructType(tempData.schema.fields.map{ f =>
      StructField(f.name, f.dataType, f.nullable)
    })

    val tempData2 = sql.createDataFrame(tempData.rdd, newSchema)

    val vectorizer = new VectorAssembler()
      .setInputCols(input_columns.diff(exclude_in_vector).toArray)
      .setOutputCol(output_column)

    vectorizer.transform(tempData2)
  }

  def filterIdentifiers(
    data: DataFrame
  ): DataFrame = {

    val schema = data.schema

    val stages = data.columns.zip(data.schema.fields)
      .flatMap {
      case (col_name, col_info) =>
          val vals = data.select(col_name).distinct().count()

          val new_name = "_" + col_name
          col_info.dataType match {
            case StringType =>

              if (vals > 10000) {
                println(s"dropping identifier $col_name")
                List(new Dropper(col_name))
              } else {
                List()
              }
            case DoubleType | IntegerType =>
              if (vals > 10000) {
                println(s"quantizing $col_name")
                List(
                  new QuantileDiscretizer()
                    .setNumBuckets(10000)
                    .setInputCol(col_name)
                    .setOutputCol(new_name),
                  new Dropper(col_name),
                  new Renamer(new_name, col_name)
                )
              } else { List ()
              }
          }
      }

    val pipeline = new Pipeline().setStages(stages)
    println("fitting transformers")
    val trans = pipeline.fit(data)
    println("running transformers")

    trans.transform(data)
  }
}
