package BI_Homework_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.rdd.RDD

object OnlineRetail {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setAppName("Homework5")
      .setMaster("local[*]") // local mode

    val sc = new SparkContext(conf)

    var df = sc.textFile("src/main/resources/data/Online Retail.csv")

    val header = df.first()

    df = df.filter(row => row != header)

    val col = df.map(line => {
      val lines = line.split(',')
      lines(1)
    })

    val transactions: RDD[Array[String]] = col.map(s => s.trim.split('\n'))

    val fpg = new FPGrowth()
      .setMinSupport(0.001)
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

    sc.stop()
  }
}
