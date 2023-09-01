package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class A6Q2Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String](descr = "input path", required = true)
    val date = opt[String](descr = "shipped date", required = true)
    val text = opt[Boolean](descr = "text", required = false)
    val parquet = opt[Boolean](descr = "parquet", required = false)
    verify()
}

object Q2 {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new A6Q2Conf(argv)
        
        log.info("Input: " + args.input())
        log.info("Shipped Date: " + args.date())
        log.info("Text: " + args.text())
        log.info("Parquet: " + args.parquet())

        val conf = new SparkConf().setAppName("SQL Data Analytics Q2")
        val sc = new SparkContext(conf)

        val targetDate =  args.date()

        if (args.text()) {
            val lineitemPath = args.input() + "/lineitem.tbl"
            val ordersPath = args.input() + "/orders.tbl"

            val orders = sc.textFile(ordersPath).map(line => {
                val orderData = line.split("\\|")
                // (o_orderkey, o_clerk)
                (orderData(0).toLong, orderData(6))
            }
            )

            val lineitem = sc.textFile(lineitemPath).filter(line => {
                val lineitemData = line.split("\\|")
                lineitemData(10).contains(targetDate)
            })
            .map(line => {
                val lineitemData = line.split("\\|")
                // (l_orderkey, l_shipdate)
                (lineitemData(0).toLong, lineitemData(10))
            })

            val outcome = lineitem.cogroup(orders) // pair = (orderkey, (shipdate, clerk))
                .filter(pair => pair._2._1.size > 0 && pair._2._2.size > 0)
                .sortByKey()
                .take(20)
                .map(pair => (pair._2._2.head, pair._1)) // outcome = (o_clerk, o_orderkey)
                .foreach(println)
            
        } else if (args.parquet()) {
            val sparkSession = SparkSession.builder.getOrCreate
            val lineitemPath = args.input() + "/lineitem"
            val lineitemDF = sparkSession.read.parquet(lineitemPath)
            val lineitemRDD = lineitemDF.rdd

            // (l_orderkey, l_shipdate)
            val lineitem = lineitemRDD.filter(line => line(10).toString.contains(targetDate))
                .map(line => (line.getInt(0), line.getString(10)))

            val ordersPath = args.input() + "/orders"
            val ordersDF = sparkSession.read.parquet(ordersPath)
            val ordersRDD = ordersDF.rdd

            // (o_orderkey, o_clerk)
            val orders = ordersRDD.map(line => (line.getInt(0), line.getString(6)))

            // outcome = (o_clerk, o_orderkey)
            val outcome = lineitem.cogroup(orders)
                .filter(pair => pair._2._1.size > 0 && pair._2._2.size > 0)
                .sortByKey()
                .take(20)
                .map(pair => (pair._2._2.head, pair._1)) // outcome = (o_clerk, o_orderkey)
                .foreach(println)
        }
    }
}
