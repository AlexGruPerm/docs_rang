package domain

import common.{DbSchema, DepFVector, SrcDocDep, SrcDocWs}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{collect_list, concat_ws, lit, udf}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint

object KMeansModel {

  private def dsDocWcSrc(spark: SparkSession, url_string: String, schema: DbSchema): Dataset[SrcDocWs] = {
    import spark.implicits._
    val ds= spark
      .read.format("jdbc")
      .option("url", url_string)
      .option("dbtable", "Javachain_Oracle.Javachain_log")
      .option("user", schema.login).option("password", schema.password)
      .option("dbtable", s"(select ID_DOC,ID_DIM,CNT from DOC_DIM_COUNT order by ID_DOC,ID_DIM)")
      .option("numPartitions", "1")
      .option("fetchSize", "1000")
      .option("customSchema", "ID_DOC INT, ID_DIM INT, CNT LONG")
      .load()
    ds.as[SrcDocWs]
  }

  private def getDsDocDep(spark: SparkSession, url_string: String, schema: DbSchema): Dataset[SrcDocDep] = {
    import spark.implicits._
    val ds= spark
      .read.format("jdbc")
      .option("url", url_string)
      .option("dbtable", "Javachain_Oracle.Javachain_log")
      .option("user", schema.login).option("password", schema.password)
      .option("dbtable", s"(select id as id_doc,id_dep from docs where id_dep is not null order by id)")
      .option("numPartitions", "1")
      .option("fetchSize", "1000")
      .option("customSchema", "ID_DOC INT, ID_DEP INT")
      .load()
    ds.as[SrcDocDep]
  }


  def readSrcDocWcCalculateModel(spark: SparkSession, url_string: String, schema: DbSchema) :Boolean = {
    import spark.implicits._
    val dsWcSrc: Dataset[SrcDocWs] = dsDocWcSrc(spark, url_string, schema)

    //dsWcSrc.collect().foreach(println)

    val combineFutures_ : Int => Double = cnt => /*id_dim+":"+*/cnt.toDouble
    val combineFutures = udf(combineFutures_)

    val dsWcSrcFut = dsWcSrc.withColumn("FUT", combineFutures($"CNT")).select($"ID_DOC",$"FUT")

    val dsAggregated = dsWcSrcFut.groupBy($"ID_DOC")
      .agg(concat_ws(",", collect_list($"FUT")).alias("FVECTOR"))

    dsAggregated.collect().foreach(s => println(s))

    val dsDocDep: Dataset[SrcDocDep] = getDsDocDep(spark, url_string, schema)
    val dictDocsDeps :List[SrcDocDep] = dsDocDep.collect().toList
    //dictDocsDeps.foreach(s => println(s))

    val addDep_ : Int => Int = p_id_doc => {
      val idDep: Int = dictDocsDeps.filter(dd => dd.id_doc == p_id_doc).map(d => d.id_dep).head
      idDep
    }
    val addDep = udf(addDep_)

    val dfForMLP: DataFrame = dsAggregated.withColumn("LABEL", addDep($"ID_DOC"))
      .select($"LABEL", $"FVECTOR")

    val dsForMLP: Dataset[DepFVector] = dfForMLP.as[DepFVector]

    dsForMLP.foreach(s => println(s))
    println(dsForMLP.schema)


    import org.apache.spark.ml.linalg

    def strToDenseVector(s: String): org.apache.spark.ml.linalg.Vector = {
      Vectors.dense(s.split(",").map(s => s.toDouble))
    }

    val ds: Array[LabeledPoint] = dsForMLP.collect().map(lv => LabeledPoint(lv.label,strToDenseVector(lv.fvector)))

    ds.foreach(println)


    true
  }

}
