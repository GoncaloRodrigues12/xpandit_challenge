package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object part2 {
  def df_2(inputdf: DataFrame): DataFrame = {
    //desconsiderei valores "NaN"
    val df_2 = inputdf.withColumn("Rating", when(col("Rating") === "NaN", lit(0.0))
        .otherwise(col("Rating").cast("double")))
      .filter(col("Rating") >= 4.0).sort(col("Rating").desc)

    val outputPath = "./src/main/output/best_apps"
    //coalesce(1) para escrever em apenas um ficheiro
    df_2.coalesce(1).write.option("header", "true").option("delimiter", "§").mode("overwrite").csv(outputPath)

    df_2
  }
}
