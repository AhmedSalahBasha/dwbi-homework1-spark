package BI_Homework_1

// $example on$
import breeze.linalg.unique
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

// $example off$

object SimpleFPGrowth {

  def main(args: Array[String]) {


    //import spark.implicits._
    // reset spark-warehouse directory
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    System.setProperty("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val OnlineRetailPath = "src/main/resources/data/Online Retail.txt"


    val spark = SparkSession.builder
      .master("local[*]")
      .appName("HW5")
      .getOrCreate

    val conf = new SparkConf()
      .setAppName("Homework5")
      .setMaster("local[*]")  // local mode

    val sc = new SparkContext(conf)
    // $example on$
    val data = sc.textFile(OnlineRetailPath)

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(','))



    //---------------------
    //val customerRows = spark.sparkContext.parallelize(data, 4)
    //---------------------
    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)

    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    val minConfidence = 0.8
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }
}
// scalastyle:on println
