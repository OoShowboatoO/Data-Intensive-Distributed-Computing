package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math.exp
import scala.collection.mutable.Map


class SpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, model, shuffle)
    val input = opt[String](descr = "input path", required = true)
    val model = opt[String](descr = "model name", required = true)
    val shuffle = opt[Boolean](descr = "shuffle", required = false)
    verify()
}

object TrainSpamClassifier {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new SpamClassifierConf(argv)
        
        log.info("Input: " + args.input())
        log.info("Model: " + args.model())
        log.info("Shuffle: " + args.shuffle())

        val conf = new SparkConf().setAppName("Train Spam Classifier")
        val sc = new SparkContext(conf)
        val isShuffled = args.shuffle()

        val outputDir = new Path(args.model())
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

        var textFile = sc.textFile(args.input(), 1)

        // w is the weight vector (make sure the variable is within scope)
        val w = Map[Int, Double]()

        // Scores a document based on its list of features.
        def spamminess(features: Array[Int]) : Double = {
            var score = 0d
            features.foreach(f => if (w.contains(f)) score += w(f))
            score
        }

        // This is the main learner:
        val delta = 0.002

        // Randomly shuffle the training instances before running the trainer if shuffle is true
        if (isShuffled) {
			textFile = textFile
				.map(row => (scala.util.Random.nextInt(), row))
				.sortByKey()
				.map(row => row._2)
		}

        // For each instance...
        val trained = textFile.map( line => {
            val data = line.split(" ")
            val id = data(0)
            // label
            val isSpam = if (data(1).equals("spam")) 1 else 0
            // feature vector of the training instance
            val features = data.drop(2).map(_.toInt)
            (0, (id, isSpam, features) )
        }).groupByKey(1)
        .flatMap(pair => {
            pair._2.foreach(data => {
                // Update the weights as follows:
                val isSpam = data._2.toDouble
                val features = data._3
                val score = spamminess(features)
                val prob = 1.0 / (1 + exp(-score))
                features.foreach(f => {
                    if (w.contains(f)) {
                        w(f) += (isSpam - prob) * delta
                    } else {
                        w(f) = (isSpam - prob) * delta
                    }
                })
            })
            w
        })
        
    trained.saveAsTextFile(args.model())
    }
}