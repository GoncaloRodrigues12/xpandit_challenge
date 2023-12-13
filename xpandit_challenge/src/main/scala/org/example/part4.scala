package org.example

import org.apache.spark.sql.DataFrame
import org.example.part1.df_1
import org.example.part3.df_3

object part4 {
  def exercice(inputdf1: DataFrame, inputdf3: DataFrame): DataFrame = {
    val df = df_3(inputdf3).join(df_1(inputdf1), Seq("App"))
    val outputPath_googleplaystore_cleaned = "./src/main/output/googleplaystore_cleaned"
    df.coalesce(1)
      .write
      .option("header", "true")
      .option("compression", "gzip")
      .mode("overwrite")
      .parquet(outputPath_googleplaystore_cleaned)
    df
  }
}
