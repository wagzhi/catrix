package catrix.model

import java.nio.ByteBuffer
import java.util.Date

import catrix.Connection

/**
  * Created by paul on 2017/9/21.
  */
case class Model8(sid:Int,uid:Int,name:String,numbers:Seq[Long],image:Array[Byte], content:Option[String],deleted:Boolean,createdAt:Date)
class Model8Table(implicit conn:Connection) extends Table[Model8]("model8") {
  val sid = column[Int]("sid")
  val uid = column[Int]("uid")
  val name = column[String]("name")
  val numbers = listColumn[Long]("numbers").index
  val image = column[Array[Byte]]("image")
  val content = column[String]("content").option.index
  val deleted = column[Boolean]("deleted")
  val createdAt = column[Date]("created_at")
  override lazy val primaryKey = partitionKeys(sid).clusteringKeys(uid).orderBy(uid Asc)
  val parser =
    (sid ~ uid ~ name ~ numbers ~  image ~ content  ~ deleted ~ createdAt <> (Model8.tupled, Model8.unapply))

  def add(s: Model8) = {
    super.insert(s).execute
  }

  def all() = {
    select(*).execute.pageResult
  }

  def getByContent(c:String) =
    select(*).filter(this.content == Some(c)).execute.pageResult



}
