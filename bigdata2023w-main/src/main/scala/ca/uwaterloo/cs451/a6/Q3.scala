package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class A6Q3Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String](descr = "input path", required = true)
    val date = opt[String](descr = "shipped date", required = true)
    val text = opt[Boolean](descr = "text", required = false)
    val parquet = opt[Boolean](descr = "parquet", required = false)
    verify()
}

object Q3 {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new A6Q3Conf(argv)
        
        log.info("Input: " + args.input())
        log.info("Shipped Date: " + args.date())
        log.info("Text: " + args.text())
        log.info("Parquet: " + args.parquet())

        val conf = new SparkConf().setAppName("SQL Data Analytics Q3")
        val sc = new SparkContext(conf)

        val targetDate =  args.date()

        if (args.text()) {
            val lineitemPath = args.input() + "/lineitem.tbl"
            val partPath = args.input() + "/part.tbl"
            val supplierPath = args.input() + "/supplier.tbl"

            // find lineitems contain the target date
            val lineitem = sc.textFile(lineitemPath)
                .filter(line => {
                    val lineitemData = line.split("\\|")
                    lineitemData(10).contains(targetDate)
                })
                .map(line => {
                    val lineitemData = line.split("\\|")
                    (lineitemData(0), lineitemData(1), lineitemData(2))
                })

            val part = sc.textFile(partPath)
                .map(line => {
                    val partData = line.split("\\|")
                    // (p_partkey, p_name)
                    (partData(0), partData(1))
                }).collectAsMap()

            val supplier = sc.textFile(supplierPath)
                .map(line => {
                    val supplierData = line.split("\\|")
                    // (s_suppkey, s_name)
                    (supplierData(0), supplierData(1))
                }).collectAsMap()

            val bcPart = sc.broadcast(part)
            val bcSupplier = sc.broadcast(supplier)

            // outcome = (l_orderkey, p_name, s_name)
            val outcome = lineitem.filter(line => {
                val partkey = line._2
                val suppkey = line._3
                bcPart.value.contains(partkey) && bcSupplier.value.contains(suppkey)
            })
            .map(line => {
                val orderkey = line._1.toLong
                val partkey = line._2
                val suppkey = line._3
                val pName = bcPart.value(partkey)
                val sName = bcSupplier.value(suppkey)
                (orderkey, (pName, sName))
            })
            .sortByKey()
            .take(20)
            .map(pair => (pair._1, pair._2._1, pair._2._2))
            .foreach(println)
            
        } else if (args.parquet()) {
            val sparkSession = SparkSession.builder.getOrCreate
            val lineitemPath = args.input() + "/lineitem"
            val lineitemDF = sparkSession.read.parquet(lineitemPath)
            val lineitemRDD = lineitemDF.rdd

            val lineitem = lineitemRDD.filter(line => line(10).toString.contains(targetDate))
                .map(line => (line(0).toString, line(1).toString, line(2).toString))

            val partPath = args.input() + "/part"
            val partDF = sparkSession.read.parquet(partPath)
            val partRDD = partDF.rdd
            // (p_partkey, p_name)
            val part = partRDD.map(line => (line(0).toString, line(1).toString)).collectAsMap()
            val bcPart = sc.broadcast(part)

            val supplierPath = args.input() + "/supplier"
            val supplierDF = sparkSession.read.parquet(supplierPath)
            val supplierRDD = supplierDF.rdd
            // (s_suppkey, s_name)
            val supplier = supplierRDD.map(line => (line(0).toString, line(1).toString)).collectAsMap()
            val bcSupplier = sc.broadcast(supplier)

            // outcome = (l_orderkey, p_name, s_name)
            val outcome = lineitem.filter(line => {
                val partkey = line._2
                val suppkey = line._3
                bcPart.value.contains(partkey) && bcSupplier.value.contains(suppkey)
            })
            .map(line => {
                val orderkey = line._1.toLong
                val partkey = line._2
                val suppkey = line._3
                val pName = bcPart.value(partkey)
                val sName = bcSupplier.value(suppkey)
                (orderkey, (pName, sName))
            })
            .sortByKey()
            .take(20)
            .map(pair => (pair._1, pair._2._1, pair._2._2))
            .foreach(println)
        }
    }
}
