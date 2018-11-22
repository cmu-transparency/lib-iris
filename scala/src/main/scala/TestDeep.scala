package edu.cmu.spf.iris

import scala.collection.JavaConversions._

import java.io.{ File, FileWriter, BufferedWriter }

object TestDeep extends App {
  case class CmdLine(
    input_file: File = new File(".")
  )

  val parser = new scopt.OptionParser[CmdLine]("Statistics") {
    head("Statistics", "0.1")
    opt[File]('i', "in")
      .required
      .action((x, c) => c.copy(input_file = x))
  }
  parser.parse(args, CmdLine()) match {
    case Some(cmdline) => RunTestDeep(cmdline)
    case None => ()
  }

  def RunTestDeep(cmdline: CmdLine): Unit = {
    /*
    import org.platanios.tensorflow.api._
    import org.platanios.tensorflow.api.tf.learn._
    import org.platanios.tensorflow.data.loaders.MNISTLoader

    // Load and batch data using pre-fetching.
    val dataSet = MNISTLoader.load(Paths.get("/tmp"))
    val trainImages = DatasetFromSlices(dataSet.trainImages)
    val trainLabels = DatasetFromSlices(dataSet.trainLabels)
    val trainData =
      trainImages.zip(trainLabels)
        .repeat()
        .shuffle(10000)
        .batch(256)
        .prefetch(10)

    // Create the MLP model.
    val input = Input(UINT8, Shape(-1, 28, 28))
    val trainInput = Input(UINT8, Shape(-1))
    val layer = Flatten() >> Cast(FLOAT32) >>
    Linear(128, name = "Layer_0") >> ReLU(0.1f) >>
    Linear(64, name = "Layer_1") >> ReLU(0.1f) >>
    Linear(32, name = "Layer_2") >> ReLU(0.1f) >>
    Linear(10, name = "OutputLayer")
    val trainingInputLayer = Cast(INT64)
    val loss = SparseSoftmaxCrossEntropy() >> Mean()
    val optimizer = GradientDescent(1e-6)
    val model = Model(input, layer, trainInput, trainingInputLayer,
      loss, optimizer)

    // Create an estimator and train the model.
    val estimator = Estimator(model)
    estimator.train(trainData, StopCriteria(maxSteps = Some(1000000)))
     */
  }
}
