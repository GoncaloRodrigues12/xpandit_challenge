package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object part3 {
  def df_3(inputdf: DataFrame): DataFrame = {
    //alterar o nome das colunas. agrupar por "App" e "Categories" passa a ser do tipo Array[String].
    //caso exista mais que uma categoria para uma app fica registado.
    //caso existam varios "genres" recolho os que sao diferentes de todas as ocoreencias da app.
    //alterei a string data para o tipo date.
    val df_3_1 = inputdf.groupBy("App").
      agg(array_distinct(collect_list("Category")).alias("Categories"),
        first("Rating").cast("double").alias("Rating"),
        max("Reviews").cast("long").alias("Reviews"),
        first("Size").alias("Size"),
        first("Installs").alias("Installs"),
        first("Type").alias("Type"),
        first("Price").alias("Price"),
        first("Content Rating").alias("Content_Rating"),
        array_distinct(split(concat_ws(";", collect_list("Genres")), ";")).alias("Genres"),
        to_date(first("Last Updated"), "MMMM dd, yyyy").alias("Last_Updated"),
        first("Current Ver").alias("Current_Version"),
        first("Android Ver").alias("Minimum_Android_Version"))

    //Na coluna size converto String para Double, caso acabe em "M" apenas dou cast, caso acabe em "k" multiplico por 1024.
    //"null" para valores como "Varies with device"
    val df_3_2 = df_3_1.withColumn("Size", when(col("Size").contains("M"), regexp_extract(col("Size"), "([0-9]+\\.?[0-9]*)M", 1).cast("double"))
      .otherwise(when(col("Size").contains("k"), regexp_extract(col("Size"), "([0-9]+\\.?[0-9]*)k", 1) / 1024.0).cast("double")))

    //Converto dolar para euros ($1 = 0.9eur).
    val df_3 = df_3_2.withColumn("Price",when(col("Price").contains("$"), regexp_extract(col("Price"), "([0-9]+\\.?[0-9]*)", 1).cast("double") * 0.9)
      .otherwise(0))

    df_3
  }
}