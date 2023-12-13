package org.example

import org.apache.spark.sql.{SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class part1Spec extends AnyFlatSpec with Matchers{

  "df_1" should "calculate Average_Sentiment_Polarity correctly" in {
    val spark = SparkSession.builder().master("local[1]").appName("part1").getOrCreate()

    import spark.implicits._

    val testData = Seq(
      ("App1", "nan"),
      ("App1", "0.9"),
      ("App2", "0.5"),
      ("App2", "0.25")
    )

    val testDF = testData.toDF("App", "Sentiment_Polarity")

    val expectedData = Seq(
      ("App1", 0.45),  // (0.9 + 0.0)/2
      ("App2", 0.375)  // (0.5 + 0.25)/2
    )

    val expectedDF = expectedData.toDF("App", "Average_Sentiment_Polarity")

    val resultDF = part1.df_1(testDF)

    resultDF.collect() should contain theSameElementsAs expectedDF.collect()

    resultDF.collect().toSeq should contain theSameElementsAs expectedDF.collect().toSeq
  }
}
