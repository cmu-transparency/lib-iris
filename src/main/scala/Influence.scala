package edu.cmu.spf.iris

import scala.math._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import Types.Precision

/** Influence measures and related computations. */
object Influence {

  /** Modes of influence computation. Single are unary or group
    * modes. */
  trait SingleMode

  /** Unary QII */
  case class Unary(col: Column) extends SingleMode

  /** Group QII */
  case class Group(cols: Seq[Column]) extends SingleMode

  /** Mode of influence computation. Multi are modes like Shapley. */
  trait MultiMode

  /** Shapley QII */
  case class Shapley() extends MultiMode

  /**
   * Single QII computations attributing influence to only to the
   * given feature/featureset.
   *
   *  @param mode the feature/featureset selection
   *  @param prec precision selection
   */
  class SingleQII(
    val mode: SingleMode,
    val prec: Precision = Precision.Full()
  ) {
    /**
     * Compute the influence.
     *
     *  @param data Input dataset.
     *  @return the influence of the selected feature/featureset.
     */
    def run(data: DataFrame): Double = {
      0.0
    }
  }

  /**
   * Multi QII computations return the influence of multiple
   * features/featuresets at once.
   *
   *  @param mode the feature/featureset selection
   *  @param prec precision selection
   */
  class MultiQII(
    val mode: MultiMode,
    val prec: Precision = Precision.Full()
  ) {

    /**
     * Compute the influence.
     *
     *  @param data Input dataset.
     *  @return The influence of the selected features/featuresets.
     */
    def run(data: DataFrame): Map[Set[Column], Double] = {
      Map()
    }
  }
}



