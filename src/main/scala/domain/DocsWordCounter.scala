package domain

import common.{CalcAndSaveResult, Dict, Ok, WordCount}
import data.DocsSourceData
import data.DocsSourceData.{getDatasetDict, getListOfUncalDocs}
import org.apache.spark.sql.{Dataset, SparkSession}

object DocsWordCounter {
  /**
   * Read the list of Dictionary words (dict_dimension).
   * Read the list of Documents ID (uncalculated documents) from db.
   * Read Documents from db one by one and calculate count of dictionary words.
   * Save all results back in to db - doc_dim_count.
  */
  def calcAndSaveWcUncalcDocs(spark: SparkSession, url_string: String): CalcAndSaveResult = {
    import spark.implicits._

    val dictDataSet: Dataset[Dict] = getDatasetDict(spark, url_string)
    dictDataSet.take(30).foreach(r => println(s"dictDataSet row = ${r.id_dim} - ${r.word}"))

    val listOfUncalcDocs: List[Int] = getListOfUncalDocs(spark, url_string)

    listOfUncalcDocs.foreach{docId =>
      //читаем документ для рассчета.
      println(s"Current document ID = $docId =========================================================")
      val countsDataSet: Dataset[WordCount] = DocsSourceData.getWordCountByDocId(spark,url_string,docId)
      countsDataSet.sort($"count".desc).take(50).foreach(r => println(s"countsDataSet row = ${r.word} - ${r.count}"))
    }

    CalcAndSaveResult(Ok,1,None)
  }

}
