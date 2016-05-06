package learn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class LoglossSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf().setMaster("local[2]")
    .setAppName("Insurance Claim")
    .set("spark.driver.allowMultipleContexts","true")
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

  import org.scalactic._

  implicit val efEq = new Equality[Double] {
    def areEqual(a: Double, b: Any): Boolean = math.abs(a-b.asInstanceOf[Double])<5E-3
  }

  "logloss((0.5, 0.5))" should "equal log one half" in {
    val rdd = sc.parallelize(Array((0.5,0.5)))
    val loss = for {
      pair <- rdd
    } yield ClaimsPrediction.calculateErrorFactor(pair._1, pair._2)
    val logloss = loss.reduce(_+_)
    logloss === -math.log(2.0) shouldBe true
  }
}