/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.immutable.Map


class ConfStripesPMI(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "number of threshold", required = false, default = Some(1))
  verify()
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPairsPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Number of threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Stripes PMI")
    val sc = new SparkContext(conf)
    val numThreshold = args.threshold()
    val maxLen = 40

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())

    // Get the number of total lines and a unique token map
    val totalLine = textFile.count()
    val uniqueTokens = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val maxIndex = Math.min(tokens.length, maxLen)
          tokens.take(maxIndex).distinct
        } 
        else List()
      })
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collectAsMap()

    val uniqueTokenMap = sc.broadcast(uniqueTokens)

    // Generate the stripes
    val counts = textFile
      .flatMap( line => {
        val tokens = tokenize(line)
        
        if (tokens.length > 1) {
          val maxIndex = Math.min(tokens.length, maxLen)
          val realTokenList = tokens.take(maxIndex).distinct
          val lstLen = realTokenList.length
          val wordDic = scala.collection.mutable.ListBuffer[(String, Map[String, Float])]()
          for (i <- 0 until lstLen) {
            for (j <- 0 until lstLen) {
              if (i != j) {
                val unitStripe: (String, Map[String, Float]) = (realTokenList(i), Map(realTokenList(j) -> 1.0f))
                wordDic += unitStripe
              }
            }
          }
          wordDic.toList
        }
        else List()
      })
      .reduceByKey((dic1, dic2) => dic1 ++ dic2.map {
        case (k, v) => k -> (dic1.getOrElse(k, 0.0f) + v)
      })
      .map(stripe => {
        val validStripe = stripe._2
          .filter{
            case (k, v) => (v >= numThreshold)
          }
        (stripe._1, validStripe)
      })
      .filter(stripe => stripe._2.size >= 1)   
      .sortByKey()

    // Calculate PMI
    val stripesPMIRes = counts
      .map(res => {
        val probX = uniqueTokenMap.value(res._1).toFloat / totalLine.toFloat
        val stripesWithPMI = res._2
          .map{
            case (k, v) => {
              val probY = uniqueTokenMap.value(k).toFloat / totalLine.toFloat
              val probPairXY = v / totalLine.toFloat
              val PMI = Math.log10( probPairXY / (probX * probY))
              (k -> (PMI, v.toInt))
            }
          }
        (res._1, stripesWithPMI)
      })

    stripesPMIRes.saveAsTextFile(args.output())
  }
}
