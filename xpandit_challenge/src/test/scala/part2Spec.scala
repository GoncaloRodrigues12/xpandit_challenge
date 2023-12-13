package org.example

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers


class part2Spec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val spark: SparkSession = SparkSession.builder().master("local[1]").appName("Part2Spec").getOrCreate()

  override protected def afterAll(): Unit = {
    spark.stop()
  }

  "df_2" should "filter and sort DataFrame correctly" in {
    import spark.implicits._

    val testData = Seq(
      ("App1", "NaN"),
      ("App2", "4.5"),
      ("App3", "3.5")
    )

    val testDF = testData.toDF("App", "Rating")

    val resultDF = part2.df_2(testDF)

    val expectedData = Seq(
      ("App2", 4.5)
    )

    val expectedDF = expectedData.toDF("App", "Rating")

    resultDF.collect() should contain theSameElementsAs expectedDF.collect()
  }
}
