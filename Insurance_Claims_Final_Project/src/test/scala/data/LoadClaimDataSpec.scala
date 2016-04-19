package data

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.scalatest.{ FlatSpec, Matchers, BeforeAndAfterAll }
import org.apache.spark.sql.types.{ DoubleType, StructType, StructField }
import data._

class LoadClaimDataSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("Insurance Claim")
      .set("spark.driver.allowMultipleContexts", "true")
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
  }

  override protected def afterAll(): Unit = {
    try {
      sc.stop()
      sc = null
      sqlContext = null
    } finally {
      super.afterAll()
    }
  }

  "emptyCheck(Any(13.33))" should "match case 13.33" in {
    val testVal: Any = 13.33
    val r = LoadClaimData.emptyCheck(testVal)
    r should matchPattern { case 13.33 => }
  }

  "convertToArray(Seq[Any])" should "match Array(1.0,2.0,3.0)" in {
    val testVal: Seq[Any] = List(1.0, 2.0, 3.0)
    val r = LoadClaimData.convertToArray(testVal)
    r should matchPattern { case Array(1.0, 2.0, 3.0) => }
  }

  it should "match Array(1.0,2.0,3.0, 10.0)" in {
    val testVal: Seq[Any] = List(1.0, 2.0, 3.0, null)
    val r = LoadClaimData.convertToArray(testVal)
    r should matchPattern { case Array(1.0, 2.0, 3.0, 10.0) => }
  }

}