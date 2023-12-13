package org.example

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers

class part4Spec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val spark: SparkSession = SparkSession.builder().master("local[1]").config("spark.sql.legacy.timeParserPolicy", "LEGACY").appName("Part4Spec").getOrCreate()

  override protected def afterAll(): Unit = {
    spark.stop()
  }

  "exercice" should "produce the correct DataFrame" in {
    import spark.implicits._

    val inputDF3Data = Seq(
      ("Photo Editor & Candy Camera & Grid & ScrapBook", "ART_AND_DESIGN", "4.1", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up"),
      ("Another App", "PHOTOGRAPHY", "3.5", "100", "15M", "5,000+", "Paid", "$2.99", "Teen", "Photography", "February 15, 2022", "2.0.0", "5.0 and up")
    )

    val inputDF3 = inputDF3Data.toDF(
      "App", "Category", "Rating", "Reviews", "Size", "Installs", "Type", "Price", "Content Rating", "Genres", "Last Updated", "Current Ver", "Android Ver"
    )

    // Test input data for df_1
    val inputDF1Data = Seq(
      ("Photo Editor & Candy Camera & Grid & ScrapBook", "nan"),
      ("Photo Editor & Candy Camera & Grid & ScrapBook", "0.9"),
      ("Another App", "0.5"),
      ("Another App", "0.25")
    )

    val inputDF1 = inputDF1Data.toDF("App", "Sentiment_Polarity")

    val expectedData = Seq(
      ("Photo Editor & Candy Camera & Grid & ScrapBook", Array("ART_AND_DESIGN"), 4.1, 159, 19.0, "10,000+", "Free", 0.0, "Everyone", Array("Art & Design"), java.sql.Date.valueOf("2018-01-07"), "1.0.0", "4.0.3 and up", 0.45),
      ("Another App", Array("PHOTOGRAPHY"), 3.5, 100, 15.0, "5,000+", "Paid", 2.6910000000000003, "Teen", Array("Photography"), java.sql.Date.valueOf("2022-02-15"), "2.0.0", "5.0 and up", 0.375)

    )

    val expectedDF = expectedData.toDF("App", "Categories", "Rating", "Reviews", "Size", "Installs", "Type", "Price", "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version", "Average_Sentiment_Polarity")

    val resultDF = part4.exercice(inputDF1, inputDF3)

    resultDF.collect() should contain theSameElementsAs expectedDF.collect()
  }
}
