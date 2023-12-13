package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.example.part1.df_1
import org.example.part3.df_3

object part5 {
  def df_4(inputdf1: DataFrame,inputdf3: DataFrame): DataFrame = {
    val df_4 = df_3(inputdf3).join(df_1(inputdf1), Seq("App"))
      .withColumn("Genres", explode(col("Genres")))
      .groupBy("Genres")
      .agg(
        count("App").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )
    val outputPath_googleplaystore_metrics = "./src/main/output/googleplaystore_metrics"
    df_4.coalesce(1)
      .write
      .option("header", "true")
      .option("compression", "gzip")
      .mode("overwrite")
      .parquet(outputPath_googleplaystore_metrics)

    df_4
  }
}
