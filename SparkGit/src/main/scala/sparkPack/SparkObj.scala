package sparkPack


import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkObj {


	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					println
					println("==========================Original Json==============================")
					println

					val picturedf = spark.read.format("json").option("multiLine","true").load("file:///D:/BigData/data/picture.json")
					picturedf.show(false)
					picturedf.printSchema()
}
}