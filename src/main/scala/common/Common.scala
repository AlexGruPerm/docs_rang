package common

import org.slf4j.LoggerFactory

sealed trait CalcStatus
case object Ok extends CalcStatus
case object Fail extends CalcStatus

object otocLogg extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger(getClass.getName)
}

case class DbSchema(login: String, password: String)

case class File(id: Int, content: String)

case class WordCount(word: String, count: Int)

case class CalcAndSaveResult(status: CalcStatus, processedDocs: Int, errorMessage: Option[String])

case class Dict(id_dim: Int, word: String)

case class DictWCDoc(id_dim: Int, cnt: Long)

case class SrcDocWs(id_doc: Int, id_dim: Int, cnt: Long)

case class SrcDocDep(id_doc: Int, id_dep: Int)

case class DepFVector(label: Int, fvector: String)

