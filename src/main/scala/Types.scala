package edu.cmu.spf.iris

object Types {

  trait Sampling

  object Sampling {
    type DataCount = Long

    case class Count(val count: Long) extends Sampling {
      implicit def toRatio(implicit datasize: DataCount): Ratio =
        Ratio(count.toDouble / datasize.toDouble)
    }
    case class Ratio(val ratio: Double) extends Sampling {
      implicit def toCount(implicit datasize: DataCount): Count =
        Count((ratio * datasize.toDouble).toLong)
    }
  }

  trait Precision

  object Precision {
    case class Full() extends Precision;
    case class Sampled(val s: Sampling) extends Precision;
  }
}
