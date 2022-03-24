package domain

import common.{CalcAndSaveResult, DbSchema, Dict, DictWCDoc, Ok, WordCount}
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
  def calcAndSaveWcUncalcDocs(spark: SparkSession, url_string: String, schema: DbSchema): CalcAndSaveResult = {
    import spark.implicits._

    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
    prop.setProperty("user", schema.login)
    prop.setProperty("password", schema.password)

    val dictDataSet: Dataset[Dict] = getDatasetDict(spark, url_string, schema)
    dictDataSet.take(30).foreach(r => println(s"dictDataSet row = ${r.id_dim} - ${r.word}"))
    dictDataSet.createOrReplaceTempView("dict")

    val listOfUncalcDocs: List[Int] = getListOfUncalDocs(spark, url_string, schema)

    listOfUncalcDocs.foreach{docId =>
      //читаем документ для рассчета.
      println(s"Current document ID = $docId =========================================================")
      val countsDataSet: Dataset[WordCount] = DocsSourceData.getWordCountByDocId(spark,url_string,docId, schema)
      countsDataSet.sort($"count".desc).take(50).foreach(r => println(s"countsDataSet row = ${r.word} - ${r.count}"))
      countsDataSet.createOrReplaceTempView("counts")

      val dictWcRes: Dataset[DictWCDoc] = spark.sql(s"SELECT $docId as id_doc, dict.id_dim,nvl(sum(counts.count),0) as cnt " +
                        "  FROM dict left join counts " +
                        "    on dict.WORD = counts.word " +
                        " group by dict.id_dim ")
        .as[DictWCDoc]

      dictWcRes.collect().take(10).foreach(dr => println(s"dict_result id_dim = ${dr.id_dim} cnt = ${dr.cnt} "))

      dictWcRes.write.mode("append").jdbc(url_string, "doc_dim_count", prop)
    }

    CalcAndSaveResult(Ok,1,None)
  }

}
