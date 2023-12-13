package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object part1{
  def df_1(inputdf: DataFrame): DataFrame = {
    //part1
    //passei os valor "nan" para 0.0 e fiz a media
    val df_1 = inputdf.withColumn("Sentiment_Polarity", when(col("Sentiment_Polarity") === "nan", lit(0.0)).
        otherwise(col("Sentiment_Polarity").cast("double")))
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    df_1
  }
}