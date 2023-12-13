package org.example

import org.apache.spark.sql.SparkSession
import org.example.part1.df_1
import org.example.part2.df_2
import org.example.part3.df_3
import org.example.part4.exercice
import org.example.part5.df_4

object main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Challenge")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    //quote para usar os valores como um todo.
    //escape para quando apanhar a dupla aspa e nao contar como delimiter.
    val csvPath_googleplaystore_user_reviews = "./src/main/resources/csv_files/googleplaystore_user_reviews.csv"
    val inputdf_googleplaystore_user_reviews = spark.read.option("header", true).option("delimiter", ",").option("quote", "\"").csv(csvPath_googleplaystore_user_reviews)
    val csvPath_googleplaystore = "./src/main/resources/csv_files/googleplaystore.csv"
    val inputdf_googleplaystore = spark.read.option("header", true).option("delimiter", ",").option("quote", "\"").option("escape", "\"").csv(csvPath_googleplaystore)

    val part1 = df_1(inputdf_googleplaystore_user_reviews)
    val part2 = df_2(inputdf_googleplaystore)
    val part3 = df_3(inputdf_googleplaystore)
    val part4 = exercice(inputdf_googleplaystore_user_reviews, inputdf_googleplaystore)
    val part5 = df_4(inputdf_googleplaystore_user_reviews, inputdf_googleplaystore)
  }
}
