package edu.cmu.spf.iris

import scala.collection.JavaConversions._
import math._

import java.io.{File,FileWriter,BufferedWriter}

import org.apache.spark.sql.{DataFrame,Row}
import org.apache.spark._

import scala.collection.mutable.WrappedArray

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.RegexTokenizer

import StringUtils._

case class Config(
  input_movies: File = new File("."),
  num_users: Int = 100,
  num_ratings: Int = 1000,
  num_personalities: Int = 10,
  num_apparents: Int = 20,
  num_latents: Int = 0,
  seed: Option[Int] = None,
  output_ratings: File = new File("ratings.csv"),
  output_summary: File = new File("summary.txt")
) {
  override def toString() = {
    import java.lang.reflect._

    getClass().getDeclaredFields().map { field:Field =>
      field.setAccessible(true)
      field.getName() + ": " + field.getType() + " = " + field.get(this).toString()
    }.mkString("\n")
  }
}

object SynthMovieRatings extends App {
  val parser = new scopt.OptionParser[Config]("SynthMovieRatings") {
    head("SynthMovieRatings", "0.2")

    opt[File]('m', "movies")
      .action((x, c) => c.copy(input_movies = x))
      .text("CSV file containing movie information.")
      .required()

    opt[Int]('n', "num-users")
      .action((x, c) => c.copy(num_users = x))
      .text("Number of users to generate.")

    opt[Int]("num-personalities")
      .action((x, c) => c.copy(num_personalities = x))
      .text("Number of personalities.")

    opt[Int]('r', "num-ratings")
      .action((x, c) => c.copy(num_ratings = x))
      .text("Number of ratings to generate.")

    opt[Int]("num-apparents")
      .action((x, c) => c.copy(num_apparents = x))
      .text("Number of features derived from extrinsics.")

    opt[Int]("num-latents")
      .action((x, c) => c.copy(num_latents = x))
      .text("Number of features not derived from extrinsics.")

    opt[File]('o', "output")
      .action((x, c) => c.copy(output_ratings = x))
      .text("CSV file to write out movie ratings.")

    opt[File]("summary")
      .action((x, c) => c.copy(output_summary = x))
      .text("Filename of summary text to write.")

    opt[Int]('s', "seed")
      .action((x, c) => c.copy(seed = Some(x)))
      .text("Random seed.")
  }

  parser.parse(args, Config()) match {
    case Some(config) =>
      config.seed match {
        case Some(seed) => Actions.r.setSeed(seed)
        case None => ()
      }
      Actions.basic(config)
    case None => ()
  }
}


object Actions {
  import scala.util.Random

  val r = new Random()

  val MULTI_FEATS = Set("genres", "imdb_genres", "imdb_keywords")

  def basic(c:Config): Unit = {
    import SparkUtil.sql.implicits._

    val (movies: DataFrame, latents: Seq[Predicate]) = {
      val m1 = SparkUtil.csvReader.load(c.input_movies.toString).na.drop

      val splitter = (new RegexTokenizer()).setPattern("\\|")

      val m2 = MULTI_FEATS.foldLeft(m1) { (ms, colname) =>
        if (ms.columns.contains(colname)) {
          val s = splitter
            .setInputCol("___")
            .setOutputCol(colname)

          s.transform(ms.withColumnRenamed(colname, "___")).drop("___")
        } else {
          ms
        }
      }
      generate_latents(c.num_latents, m2)
    }

    val num_users = c.num_users
    val num_ratings = c.num_ratings
    val num_movies = movies.count

    val ratings_per_user = num_ratings / num_users
    val fraction_per_user = ratings_per_user.toDouble / num_movies.toDouble

    println(s"num_ratings = $num_ratings")
    println(s"num_movies = $num_movies")
    println(s"ratings_per_user = $ratings_per_user")
    println(s"fraction_per_user = $fraction_per_user")

    val props = latents ++ extract_movie_properties(c.num_apparents, movies)
    val persons = generate_personalities(c.num_personalities, props).toArray
    val users = generate_users(num_users, persons, fraction_per_user)

    val bw = new BufferedWriter(new FileWriter(c.output_summary))
    bw.write(c.toString + "\n\n")

    persons.foreach { personality =>
      bw.write(personality + "\n")
      users.toSeq
        .filter{ user => user.personality == personality }.take(5)
        .foreach{ user => bw.write(tab(user) + "\n") }
    }

    bw.close()

    val reviews = for(
      user <- users;
      pers = user.personality;
      act = min(1.0, user.activity*10.0);
      movie <- user.gen_interesting_movies(
        movies.sample(
          withReplacement = false,
          fraction = act,
          seed = r.nextLong
        ),
        num_to_return = ratings_per_user
      ).collect;
      movie_id = movie.getInt(0);
      rating = user.rate_movie(movie);
      timestamp = System.currentTimeMillis / 1000
    ) yield Row(user.id.toInt, movie_id.toInt, rating.toInt, timestamp.toLong)

    val schema: StructType = StructType(
      StructField("user", IntegerType, false) ::
      StructField("movie", IntegerType, false) ::
      StructField("rating", IntegerType, false) ::
      StructField("timestamp", LongType, true) :: Nil
    )

    val df = movies.sqlContext.createDataFrame(
      rows = reviews,
      schema = schema
    )

    SparkUtil.csvWrite(
      dataframe=df,
      file=c.output_ratings.toString,
      delimiter=":"
    )

  }

  def generate_latents(num_latents: Int,
    movies: DataFrame
  ): (DataFrame, Seq[Predicate]) = {

    val (new_rdd, new_schema, predicates) = (0 until num_latents)
      .foldLeft((
        movies.rdd,
        movies.schema,
        List[Predicate]()
      )) {
      case ((rdd, schema, predicates), latent_index) =>
        val latent_name = s"latent_$latent_index"
        (
          rdd.map { case row => Row.fromSeq(row.toSeq ++ Seq(r.nextInt.toString)) },
          schema.add(latent_name, StringType, false),
          predicates ++ List(
            new FeatureIs(latent_name, "0"),
            new FeatureIs(latent_name, "1")
          )
        )
    }
    val df = movies.sqlContext.createDataFrame(
      rowRDD = new_rdd,
      schema = new_schema
    )

    println(new_schema.toString)

    (df, predicates)
  }

  abstract class Predicate(val feature: String) extends Serializable {
    def matches(movie: Row): Boolean
  }

  class FeatureContains(_feature: String, element: String)
      extends Predicate(_feature) {
    override def toString: String = element + " ∈ " + _feature
    def matches(movie: Row): Boolean =
      movie.getAs[WrappedArray[String]](_feature).contains(element)
  }

  class FeatureIs(_feature: String, element: String)
      extends Predicate(_feature) {
    override def toString: String = element + " = " + _feature
    def matches(movie: Row): Boolean =
      movie.getAs[String](_feature) == element
  }

  class User(val id: Int, val personality: Personality, val activity: Double)
      extends Serializable {

    override def toString: String =
      f"User (id:$id%d, activity:$activity%1.3f, personality:$personality%s)"

    def gen_interesting_movies(
      movies: DataFrame,
      num_to_return: Int
    ): DataFrame = {
      import SparkUtil.sql.implicits._

      val groups = movies.rdd
        .map { case r: Row => (r, rate_movie(r)) }
        .groupBy(_._2)
        .sortBy(_._1)
        .collect

      val num_groups = groups.length

      val temp = for (
        num <- (0 to num_to_return);
        group_index = r.nextInt(num_groups);
        (group, movies_and_ratings) = groups(group_index);
        pairs: Array[(Row,Double)] = movies_and_ratings.toArray;
        rand_index = r.nextInt(pairs.length)
      ) yield pairs(rand_index)._1

      movies.sqlContext.createDataFrame(temp.toList, movies.schema)
    }

    def rate_movie(m: Row): Double = {
      val points = personality.prefs.map { case (pred, st) =>
        if (pred.matches(m)) { st } else { 0.0 }
      }.sum
      (3.0 + points).toInt.toDouble
    }
  }

  class Personality(
    val prefs: Seq[(Predicate, Double)]) extends Serializable {
    override def toString: String =
      "Personality (prefs: " + prefs.map(_.toString).mkString(",") + ")"
  }

  def generate_users(num_users: Int,
    persons: Seq[Personality],
    activityScale: Double
  ): Seq[User] = {
    val apers = persons.toArray
    for (
      user_id <- (0 to num_users);
      i = r.nextInt(apers.length);
      pers = apers(i);
      activity = min(1.0, r.nextDouble * activityScale + 0.5 * activityScale)
    ) yield new User(user_id, pers, activity)
  }

  def generate_personalities(num_persons: Int, preds: Seq[Predicate])
      : Seq[Personality] = {

    val apreds = preds.toArray
    val numpreds = apreds.length

    def rand_predicate: Predicate = {
      apreds(r.nextInt(numpreds))
    }

    (0 until num_persons).map { _ => new Personality(
      Seq((rand_predicate, 1.0),(rand_predicate,-1.0))
    ) }
  }

  def extract_movie_properties(
    num_apparents: Int,
    movies: DataFrame
  ): Seq[Predicate] = {

    import SparkUtil.sql.implicits._

    val num_movies = movies.count.toDouble

    val all_feats = movies.schema.foldLeft(List[(Predicate, Double)]()) {
      case (feats, StructField(cname, ctype, _, _)) =>

        val selected = movies.select(cname)

        ctype match {
          case IntegerType => feats

          case ArrayType(StringType,_) =>

            val multivalues = selected.flatMap {
              case Row(strings) => strings.asInstanceOf[Seq[String]]
            }

            feats ++ multivalues.groupBy("value").count.map { case r:Row =>
              (r.getAs[String]("value"),
                Information.entropy(
                  r.getAs[Long]("count").toDouble / num_movies)
              )
            }.collect
              .map { case (cat, ent) => (new FeatureContains(cname, cat), ent) }

        case StringType => feats ++
            selected
              .groupBy(cname).count.map { case r:Row =>
                (r.getAs[String](cname),
                  Information.entropy(
                    r.getAs[Long]("count").toDouble / num_movies)
                )
            }.collect
              .map { case (cat, ent) => (new FeatureIs(cname, cat), ent) }
        }
    }

    val best_feats = all_feats.sortBy(- _._2).view(0,num_apparents)

    best_feats.map(_._1)
  }

}
