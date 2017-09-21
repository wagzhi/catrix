package catrix.model

import java.nio.ByteBuffer
import java.util.Date

import catrix.Connection

/**
  * Created by paul on 2017/9/21.
  */
case class Model7(sid:Int,uid:Int,name:String,numbers:Seq[Long],image:ByteBuffer,deleted:Boolean,createdAt:Date)
class Model7Table(implicit conn:Connection) extends Table[Model7]("model7") {
  val sid = column[Int]("sid")
  val uid = column[Int]("uid")
  val name = column[String]("name")
  val numbers = listColumn[Long]("numbers").index
  val image = column[ByteBuffer]("image")
  val deleted = column[Boolean]("deleted")
  val createdAt = column[Date]("created_at")
  override lazy val primaryKey = partitionKeys(sid).clusteringKeys(uid).orderBy(uid Asc)
  val parser =
    (sid ~ uid ~ name ~ numbers ~ image ~ deleted ~ createdAt <> (Model7.tupled, Model7.unapply))

  def add(s: Model7) = {
    super.insert(s).execute
  }

  def all() = {
    select(*).execute.pageResult
  }



}
