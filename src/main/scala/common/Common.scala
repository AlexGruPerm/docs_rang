package common

import org.slf4j.LoggerFactory

sealed trait CalcStatus
case object Ok extends CalcStatus
case object Fail extends CalcStatus

object otocLogg extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger(getClass.getName)
}

case class File(id: Int, content: String)

case class WordCount(word: String, count: Int)

case class CalcAndSaveResult(status: CalcStatus, processedDocs: Int, errorMessage: Option[String])

case class Dict(id_dim: Int, word: String)

case class DictWCDoc(id_dim: Int, cnt: Long)