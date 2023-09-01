package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class A6Q4Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String](descr = "input path", required = true)
    val date = opt[String](descr = "shipped date", required = true)
    val text = opt[Boolean](descr = "text", required = false)
    val parquet = opt[Boolean](descr = "parquet", required = false)
    verify()
}

object Q4 {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new A6Q4Conf(argv)
        
        log.info("Input: " + args.input())
        log.info("Shipped Date: " + args.date())
        log.info("Text: " + args.text())
        log.info("Parquet: " + args.parquet())

        val conf = new SparkConf().setAppName("SQL Data Analytics Q4")
        val sc = new SparkContext(conf)

        val targetDate =  args.date()

        if (args.text()) {
            val lineitemPath = args.input() + "/lineitem.tbl"
            val ordersPath = args.input() + "/orders.tbl"
            val customerPath = args.input() + "/customer.tbl"
            val nationPath = args.input() + "/nation.tbl"

            // find lineitems contain the target date
            val lineitem = sc.textFile(lineitemPath)
                .filter(line => {
                    val lineitemData = line.split("\\|")
                    lineitemData(10).contains(targetDate)
                })
                .map(line => {
                    val lineitemData = line.split("\\|")
                    // (l_orderkey, l_shipdate)
                    (lineitemData(0), lineitemData(10))
                })

            val orders = sc.textFile(ordersPath)
                .map(line => {
                    val orderData = line.split("\\|")
                    // (o_orderkey, o_custkey )
                    (orderData(0), orderData(1))
                })

            val customer = sc.textFile(customerPath)
                .map(line => {
                    val customerData = line.split("\\|")
                    // (c_custkey, c_nationkey)
                    (customerData(0), customerData(3))
                }).collectAsMap()

            val nation = sc.textFile(nationPath)
                .map(line => {
                    val nationData = line.split("\\|")
                    // (n_nationkey, n_name)
                    (nationData(0), nationData(1))
                }).collectAsMap()

            val bcNation = sc.broadcast(nation)
            val bcCustomer = sc.broadcast(customer)

            val orderNationPair = orders.filter(orderData => {
                bcCustomer.value.contains(orderData._2)
            })
            // => (o_orderkey, c_nationkey)
            .map(orderData => (orderData._1, bcCustomer.value(orderData._2))) 

            // outcome = (n_nationkey, n_name, count(*))
            val outcome = lineitem.cogroup(orderNationPair) // => (l_orderkey, (List l_shipdate, List c_nationkey))
                .filter(data => data._2._1.size != 0 && data._2._2.size != 0)
                .flatMap(line => {for (shipedDate <- line._2._1) yield (line._1, (shipedDate, line._2._2))})
                .filter(line => bcNation.value.contains(line._2._2.head)) // make sure the corresponding n_name existing
                .map(line => ((line._2._2.head, bcNation.value(line._2._2.head)), 1)) // group by n_nationkey, n_name
                .reduceByKey(_ + _)
                .map(line => (line._1._1.toLong, line._1._2, line._2))
                .sortBy(_._1)
                .collect()
                .foreach(println)
            
        } else if (args.parquet()) {
            val sparkSession = SparkSession.builder.getOrCreate
            
            val lineitemPath = args.input() + "/lineitem"
            val lineitemDF = sparkSession.read.parquet(lineitemPath)
            val lineitemRDD = lineitemDF.rdd
            // (l_orderkey, l_shipdate)
            val lineitem = lineitemRDD.filter(line => line(10).toString.contains(targetDate))
                .map(line => (line(0).toString, line(10).toString))

            val ordersPath = args.input() + "/orders"
            val ordersDF = sparkSession.read.parquet(ordersPath)
            val ordersRDD = ordersDF.rdd
            // (o_orderkey, o_custkey )
            val orders = ordersRDD.map(line => (line(0).toString, line(1).toString))

            val customerPath = args.input() + "/customer"
            val customerDF = sparkSession.read.parquet(customerPath)
            val customerRDD = customerDF.rdd
            // (s_suppkey, s_name)
            val customer = customerRDD.map(line => (line(0).toString, line(3).toString)).collectAsMap()
            val bcCustomer = sc.broadcast(customer)

            val nationPath = args.input() + "/nation"
            val nationDF = sparkSession.read.parquet(nationPath)
            val nationRDD = nationDF.rdd
            val nation = nationRDD.map(line => (line(0).toString, line(1).toString)).collectAsMap()
            val bcNation = sc.broadcast(nation)

            val orderNationPair = orders.filter(orderData => {
                bcCustomer.value.contains(orderData._2)
            })
            // => (o_orderkey, c_nationkey)
            .map(orderData => (orderData._1, bcCustomer.value(orderData._2)))  
            
            // outcome = (n_nationkey, n_name, count(*))
            val outcome = lineitem.cogroup(orderNationPair) // => (l_orderkey, (List l_shipdate, List c_nationkey))
                .filter(data => data._2._1.size != 0 && data._2._2.size != 0)
                .flatMap(line => {for (shipedDate <- line._2._1) yield (line._1, (shipedDate, line._2._2))})
                .filter(line => bcNation.value.contains(line._2._2.head)) // make sure the corresponding n_name existing
                .map(line => ((line._2._2.head, bcNation.value(line._2._2.head)), 1))
                .reduceByKey(_ + _)
                .map(line => (line._1._1.toLong, line._1._2, line._2))
                .sortBy(_._1)
                .collect()
                .foreach(println)
        }
    }
}