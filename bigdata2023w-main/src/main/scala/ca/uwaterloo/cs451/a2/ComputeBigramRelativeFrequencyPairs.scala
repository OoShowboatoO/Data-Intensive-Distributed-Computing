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

import org.apache.spark.Partitioner
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._


class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  verify()
}

class MyPartitioner(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = key match {
      case (part1: String, part2: String)  => {
          (part1.hashCode() & Integer.MAX_VALUE) % numPartitions
      }   
    }
  }

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Pairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          // Generate a counter pair and a normal pair
          val countPair = tokens.sliding(2).map(p => (p(0), "*")).toList
          val normalPair = tokens.sliding(2).map(p => (p(0), p(1))).toList
          normalPair ::: countPair
        } 
        else List()
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .repartitionAndSortWithinPartitions(new MyPartitioner(args.reducers()))
      
      .mapPartitions(part => {
        var marginal = 0.0f
        part.map(pair => {
          // Obtain the number of marginal from counter pairs
          if (pair._1._2.equals("*")) {
            marginal = pair._2.toFloat
            (pair._1, marginal)
          } else {
            // Compute Relative Frequency for normal pairs
            (pair._1, pair._2.toFloat / marginal)
          }
        })
      })
    counts.saveAsTextFile(args.output())
  }
}
