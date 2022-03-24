package data

import common.{DbSchema, Dict, File, WordCount, otocLogg}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object DocsSourceData {

  private def dsDoc(spark: SparkSession, url_string: String, docId: Int, schema: DbSchema): Dataset[File] = {
    import spark.implicits._
    val ds= spark
      .read.format("jdbc")
      .option("url", url_string)
      .option("dbtable", "Javachain_Oracle.Javachain_log")
      .option("user", schema.login).option("password", schema.password)
      .option("dbtable", s"(select ID,replace(CONTENT,',','') as CONTENT from DOCS where id = ${docId})")
      .option("numPartitions", "1")
      .option("fetchSize", "1")
      .option("customSchema", "ID INT,CONTENT STRING")
      .load()
    ds.as[File]
  }

  def getDatasetDict(spark: SparkSession, url_string: String, schema: DbSchema): Dataset[Dict] = {
    import spark.implicits._
    val ds= spark
      .read.format("jdbc")
      .option("url", url_string)
      .option("dbtable", "Javachain_Oracle.Javachain_log")
      .option("user", schema.login).option("password", schema.password)
      .option("dbtable", s"(select d.id_dim,d.word from dict_dimension d order by 1)")
      .option("numPartitions", "1")
      .option("fetchSize", "100")
      .option("customSchema", "ID_DIM INT,WORD STRING")
      .load()
    ds.as[Dict]
  }

  def getListOfUncalDocs(spark: SparkSession, url_string: String, schema: DbSchema): List[Int] = {
    import spark.implicits._
    val ds = spark
      .read.format("jdbc")
      .option("url", url_string)
      .option("dbtable", "Javachain_Oracle.Javachain_log")
      .option("user", schema.login).option("password", schema.password)
      .option("dbtable", s"(select d.ID from docs d where not exists(select 1 from doc_dim_count r where r.id_doc=d.id))")
      .option("numPartitions", "1")
      .option("fetchSize", "100")
      .option("customSchema", "ID INT")
      .load()
    ds.as[Int].collect().toList
  }

  def getWordCountByDocId(spark: SparkSession, url_string: String, docId: Int, schema: DbSchema): Dataset[WordCount] ={
    import spark.implicits._
    val srcDataSet: Dataset[File] = dsDoc(spark,url_string,docId,schema)
    //srcDataSet.foreach(d => println(s" id = ${d.id}  content = ${d.content}"))

    val documentId: Int = srcDataSet.head().id

    //otocLogg.log.info(s"documentId = $documentId")

    val content: String = srcDataSet.collect().head.content

    val counts: List[(String,Int)] = srcDataSet.collect().head.content.split(" ")
      .map(word => (word,1)).toList

    val rdd: RDD[(String,Int)] = spark.sparkContext.parallelize(counts).reduceByKey(_ + _)

    val countsDataSet: Dataset[WordCount] = rdd.toDF("word", "count").as[WordCount]
    countsDataSet
  }


}
