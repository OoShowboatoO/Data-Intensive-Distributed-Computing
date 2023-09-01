package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class A6Q1Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String](descr = "input path", required = true)
    val date = opt[String](descr = "shipped date", required = true)
    val text = opt[Boolean](descr = "text", required = false)
    val parquet = opt[Boolean](descr = "parquet", required = false)
    verify()
}

object Q1 {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new A6Q1Conf(argv)
        
        log.info("Input: " + args.input())
        log.info("Shipped Date: " + args.date())
        log.info("Text: " + args.text())
        log.info("Parquet: " + args.parquet())

        val conf = new SparkConf().setAppName("SQL Data Analytics Q1")
        val sc = new SparkContext(conf)

        val targetDate =  args.date()

        if (args.text()) {
            val filePath = args.input() + "/lineitem.tbl"
            val textFile = sc.textFile(filePath)
            val count = textFile.map(line => {
                val tokens = line.split("\\|")
                tokens(10)
            })
            .filter(line => line.contains(targetDate))
            .count

            println("ANSWER=" + count)

        } else if (args.parquet()) {
            val sparkSession = SparkSession.builder.getOrCreate
            val parquetFilePath = args.input() + "/lineitem"
            val lineitemDF = sparkSession.read.parquet(parquetFilePath)
            val lineitemRDD = lineitemDF.rdd
            val count = lineitemRDD.map(line => line.getString(10))
            .filter(line => line.contains(targetDate))
            .count

            println("ANSWER=" + count)
        }
    }
}
