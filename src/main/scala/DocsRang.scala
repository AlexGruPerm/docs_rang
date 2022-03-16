import org.apache.spark.sql.SparkSession
import common._
import domain.DocsWordCounter

object DocsRang extends App {
  otocLogg.log.info("BEGIN [OraToCass]")
  val url_string: String = "jdbc:oracle:thin:"+"MSK_ARM_LEAD"+"/"+"MSK_ARM_LEAD"+"@//"+"10.127.24.11:1521/test"

  val spark: SparkSession = SparkSession.builder()
    .appName("docsrang")
    .config("spark.master", "local")
    .config("spark.jars", "/root/docs_rang_v1.jar")
    .getOrCreate()

  val t1_common = System.currentTimeMillis

  val resWcCalculation: CalcAndSaveResult = DocsWordCounter.calcAndSaveWcUncalcDocs(spark, url_string)

  otocLogg.log.info("Finish application")

  val t2_common = System.currentTimeMillis
  otocLogg.log.info("================== SUMMARY ========================================")
  otocLogg.log.info(" DURATION :"+ ((t2_common - t1_common)/1000.toDouble) + " sec.")
  otocLogg.log.info("================== END [OraToCass] ================================")
}
