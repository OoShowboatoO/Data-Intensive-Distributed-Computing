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


class ConfStripes(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  verify()
}

class MyPartitionerStripes(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = key match {
      case (word: String) => {
        (word.hashCode() & Integer.MAX_VALUE) % numPartitions
      }
    }
  }

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfStripes(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Stripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        // Form Unit Stripes for each pairs in a line
        if (tokens.length > 1) tokens.sliding(2).map(p => (p(0), Map(p(1) -> 1.0f))) else List()
      })
      
      // Merge dictionaries/maps into 1 dictionary/map
      .reduceByKey((dic1, dic2) => dic1 ++ dic2.map {
        case (k, v) => k -> (dic1.getOrElse(k, 0.0f) + v)
      })
      .repartitionAndSortWithinPartitions(new MyPartitionerStripes(args.reducers()))
      
      // Compute Relative Frequency for each element in stripes
      .mapPartitions(part => {
        part.map(word => {          
          val sum = word._2.foldLeft(0.0f)(_+_._2) // Get the sum of all values in the stripe
          val relativeFrequency = word._2.map{case (k, v) => (k, v/sum)}
          (word._1, relativeFrequency)
        })
      })
      
    counts.saveAsTextFile(args.output())
  }
}
