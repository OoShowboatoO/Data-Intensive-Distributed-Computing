package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ApplyEnsembleSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, model, method)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val model = opt[String](descr = "model name", required = true)
    val method = opt[String](descr = "method", required = true)
    verify()
}

object ApplyEnsembleSpamClassifier {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new ApplyEnsembleSpamClassifierConf(argv)

        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Model: " + args.model())
        log.info("Method: " + args.method())

        val conf = new SparkConf().setAppName("Apply Ensemble Spam Classifier")
		val sc = new SparkContext(conf)

		val outputDir = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

        val method = args.method()
        val textFile = sc.textFile(args.input(), 1)
        
        val groupXPath = args.model() + "/part-00000"
        val groupYPath = args.model() + "/part-00001"
        val britneyPath = args.model() + "/part-00002"

        def getWeights(modelPath: String) : Map[Int, Double] = {
            val model = sc.textFile(modelPath)
            model.map(line => {
                val pair = line.substring(1, line.length() - 1).split(",")
                val featureId = pair(0).toInt
                val score = pair(1).toDouble
                (featureId, score)
            })
            .collect().toMap
        }

        val wGroupX = sc.broadcast(getWeights(groupXPath))
        val wGroupY = sc.broadcast(getWeights(groupYPath))
        val wBritney = sc.broadcast(getWeights(britneyPath))
        
        def spamminess(features: Array[Int], weights: Map[Int, Double]) : Double = {
			var score = 0d
			features.foreach(f => if (weights.contains(f)) score += weights(f))
			score
		}

        val result = textFile.map(line => {
            var finalScore = 0d
            var classification = "NaN"
            
            val rowData = line.split(" ")
            val docId = rowData(0)
            val isSpamFromOG = rowData(1)
            val features = rowData.drop(2).map(_.toInt)
            val Xscore = spamminess(features, wGroupX.value)
			val Yscore = spamminess(features, wGroupY.value)
			val britneyScore = spamminess(features, wBritney.value)

            if (method.equals("vote")) {
                val Xvote = if (Xscore > 0) 1d else -1d
			    val Yvote = if (Yscore > 0) 1d else -1d
			    val britneyVote = if (britneyScore > 0) 1d else -1d
                finalScore = britneyVote + Yvote + Xvote
            } else {
                finalScore = (britneyScore + Yscore + Xscore) / 3d
            }
            
            if (finalScore > 0) {
                classification = "spam"
            } else {
                classification = "ham"
            }
            
            (docId, isSpamFromOG, finalScore, classification)
        })
        result.saveAsTextFile(args.output())
    }
}
