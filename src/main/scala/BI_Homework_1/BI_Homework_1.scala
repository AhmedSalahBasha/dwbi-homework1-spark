package BI_Homework_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object BI_Homework_1 {
  def main(args: Array[String]) {

    // reset spark-warehouse directory
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    System.setProperty("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("BI-Homework1")
      .getOrCreate


    val OnlineRetailPath = "src/main/resources/data/Online Retail.csv"

    val onlineRetailDF = spark.read
        .format("csv")
        .option("header", "true")
        .load(OnlineRetailPath)

    onlineRetailDF.show()
    /*
    //onlineRetailDF.show(false)

    //val transactions = onlineRetailDF.map(row => Row(row(0), row(1)))

    //val rows: RDD[Row] = onlineRetailDF.rdd
    val transactions: RDD[Array[String]] = onlineRetailDF.map(line => line.split(",").map(_.trim))


    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)

    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)

    }*/
  }
}
