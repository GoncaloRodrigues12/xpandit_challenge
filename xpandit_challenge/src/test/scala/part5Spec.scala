package org.example

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers

class part5Spec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val spark: SparkSession = SparkSession.builder().master("local[1]").config("spark.sql.legacy.timeParserPolicy", "LEGACY").appName("Part4Spec").getOrCreate()

  override protected def afterAll(): Unit = {
    spark.stop()
  }
  "df_4" should "produce the correct DataFrame" in {
    import spark.implicits._

    val inputDF3Data = Seq(
      ("Photo Editor & Candy Camera & Grid & ScrapBook", "ART_AND_DESIGN", "4.1", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up"),
      ("Another App", "PHOTOGRAPHY", "3.5", "100", "15M", "5,000+", "Paid", "$2.99", "Teen", "Photography", "February 15, 2022", "2.0.0", "5.0 and up")
    )

    val inputDF3 = inputDF3Data.toDF(
      "App", "Category", "Rating", "Reviews", "Size", "Installs", "Type", "Price", "Content Rating", "Genres", "Last Updated", "Current Ver", "Android Ver"
    )

    val inputDF1Data = Seq(
      ("Photo Editor & Candy Camera & Grid & ScrapBook", "nan"),
      ("Photo Editor & Candy Camera & Grid & ScrapBook", "0.9"),
      ("Another App", "0.5"),
      ("Another App", "0.25")
    )

    val inputDF1 = inputDF1Data.toDF("App", "Sentiment_Polarity")

    val expectedData = Seq(
      ("Photography", 1, 3.5, 0.375),
      ("Art & Design", 1, 4.1, 0.45)
    )
    val expectedDF = expectedData.toDF("Genres", "Count", "Average_Rating", "Average_Sentiment_Polarity")

    val resultDF = part5.df_4(inputDF1, inputDF3)

    resultDF.collect() should contain theSameElementsAs expectedDF.collect()
  }
}
