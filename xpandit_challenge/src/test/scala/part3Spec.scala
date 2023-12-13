package org.example

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers


class part3Spec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val spark: SparkSession = SparkSession.builder().master("local[1]").config("spark.sql.legacy.timeParserPolicy", "LEGACY").appName("Part3Spec").getOrCreate()

  override protected def afterAll(): Unit = {
    spark.stop()
  }

  "df_3" should "transform DataFrame correctly" in {
    import spark.implicits._

    val testData = Seq(
      ("Photo Editor & Candy Camera & Grid & ScrapBook", "ART_AND_DESIGN", "4.1", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up"),
      ("Another App", "PHOTOGRAPHY", "3.5", "100", "15M", "5,000+", "Paid", "$2.99", "Teen", "Photography", "February 15, 2022", "2.0.0", "5.0 and up")
    )

    val testDF = testData.toDF(
      "App", "Category", "Rating", "Reviews", "Size", "Installs", "Type", "Price", "Content Rating", "Genres", "Last Updated", "Current Ver", "Android Ver"
    )

    val expectedData = Seq(
      ("Photo Editor & Candy Camera & Grid & ScrapBook", Array("ART_AND_DESIGN"), 4.1, 159, 19.0, "10,000+", "Free", 0.0, "Everyone", Array("Art & Design"), java.sql.Date.valueOf("2018-01-07"), "1.0.0", "4.0.3 and up"),
      ("Another App", Array("PHOTOGRAPHY"), 3.5, 100, 15.0, "5,000+", "Paid", 2.6910000000000003, "Teen", Array("Photography"), java.sql.Date.valueOf("2022-02-15"), "2.0.0", "5.0 and up")

    )

    val expectedDF = expectedData.toDF("App", "Categories", "Rating", "Reviews", "Size", "Installs", "Type", "Price", "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version")

    val resultDF = part3.df_3(testDF)

    resultDF.collect() should contain theSameElementsAs expectedDF.collect()
  }
}
