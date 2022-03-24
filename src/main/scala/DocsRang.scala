import org.apache.spark.sql.SparkSession
import common._
import domain.{DocsWordCounter, KMeansModel}

object DocsRang extends App {
  otocLogg.log.info("BEGIN [OraToCass]")
  val url_string: String = "jdbc:oracle:thin:"+"npa_docs"+"/"+"npa_docs"+"@//"+"10.127.24.11:1521/test"
  val schema: DbSchema = DbSchema("npa_docs","npa_docs")

  val spark: SparkSession = SparkSession.builder()
    .appName("docsrang")
    .config("spark.master", "local")
    .config("spark.jars", "/root/docs_rang_v1.jar")
    .getOrCreate()

  val t1_common = System.currentTimeMillis

  //Функция читает список ид необработанных документов, считает по ним w.c. и сохраняет результаты в DOC_DIM_COUNT
  // --------- val resWcCalculation: CalcAndSaveResult = DocsWordCounter.calcAndSaveWcUncalcDocs(spark, url_string, schema)

  //Читаем посчитанные данные из таблицы., и строим k-means модель.
  val kMeansModelResult = KMeansModel.readSrcDocWcCalculateModel(spark, url_string, schema)

  otocLogg.log.info("Finish application")

  val t2_common = System.currentTimeMillis
  otocLogg.log.info("================== SUMMARY ========================================")
  otocLogg.log.info(" DURATION :"+ ((t2_common - t1_common)/1000.toDouble) + " sec.")
  otocLogg.log.info("================== END ============================================")
}
