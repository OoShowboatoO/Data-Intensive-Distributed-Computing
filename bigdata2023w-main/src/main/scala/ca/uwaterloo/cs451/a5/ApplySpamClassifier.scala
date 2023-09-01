package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model name", required = true)
  verify()
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ApplySpamClassifierConf(argv)
    
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
	
	  val conf = new SparkConf().setAppName("Apply Spam Classifier")
    val sc = new SparkContext(conf)
    
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // Obtain all (featrue, score) pairs
    val modelPath = args.model() + "/part-00000"
    val textFile = sc.textFile(modelPath, 1)
    val modelData = textFile.map(line => {
        val pair = line.substring(1, line.length() - 1).split(",")
        val featureId = pair(0).toInt
        val score = pair(1).toDouble
        (featureId, score)
    })
    .collect().toMap

    val broadcastData = sc.broadcast(modelData)

    def spamminess(features: Array[Int]) : Double = {
        var score = 0d
        features.foreach(f => {
          val w = broadcastData.value
          if (w.contains(f)) score += w(f)
        })
        score
    }

    // Obtain the original data as test data, and make comparisons
    val comparison = sc.textFile(args.input(), 1).map(line => {
        val rowData = line.split(" ")
        val docId = rowData(0)
        val isSpamFromOG = rowData(1)
        val features = rowData.drop(2).map(_.toInt)
        val scoreFromModel = spamminess(features)
        val classification  = if (scoreFromModel > 0) "spam" else "ham"
        (docId, isSpamFromOG, scoreFromModel, classification)
    })

    comparison.saveAsTextFile(args.output())
  }
}
