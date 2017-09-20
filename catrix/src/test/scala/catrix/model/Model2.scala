package catrix.model

import java.util.Date

import catrix.model.{RowParser2, Table}
import catrix.{Connection}

/**
  * Created by paul on 2017/9/18.
  */
case class Model2(id:Int,name:String)

class Model2Table(implicit conn:Connection) extends Table[Model2]("model2") {
  val id = column[Int]("id")
  val name = column[String]("name").index
  override lazy val primaryKey = partitionKeys(id).clusteringKeys(name).orderBy(name Desc)
  val parser =
    (id ~ name <> (Model2.tupled, Model2.unapply))

  def add(s: Model2) = {
    super.insert(s).execute
  }

  def getByName(name: String) = {
    select(*).filter(this.name == name).execute.pageResult.results.headOption
  }

  def getByIds(ids:Int*) = {
    select(*).filter(this.id in ids).execute.pageResult
  }

  def all() = {
    select(*).execute.pageResult
  }
}


case class Model3(id:Int,name:String,tags:Seq[String])

class Model3Table(implicit conn:Connection) extends Table[Model3]("model3") {
  val id = column[Int]("id")
  val name = column[String]("name")
  val tags = listColumn[String]("tags").index
  override lazy val primaryKey = partitionKeys(id).clusteringKeys(name).orderBy(name Desc)
  val parser =
    (id ~ name ~ tags <> (Model3.tupled, Model3.unapply))

  def add(s: Model3) = {
    super.insert(s).execute
  }

  def getById(id: Int) = {
    select(*).filter(this.id == 1).execute.pageResult
  }

  def all() = {
    select(*).execute.pageResult
  }

  def getByTags(tag:String) = {
    this.select(*).filter(this.tags contains tag).execute.pageResult
  }
}


object Gender extends Enumeration{
  type Gender = Value
  val Man,Woman,UnknownGender = Value


}
import Gender._
case class Model4(id:Int,name:String,gender:Gender,createdAt:Date)

class Model4Table(implicit conn:Connection) extends Table[Model4]("model4") {
  val id = column[Int]("id")
  val name = column[String]("name")
  val gender = enumColumn[Gender]("gender").default(UnknownGender).index
  val createdAt = column[Date]("created_at")
  override lazy val primaryKey = partitionKeys(id).clusteringKeys(createdAt).orderBy(createdAt Desc)
  val parser =
    (id ~ name ~ gender ~ createdAt <> (Model4.tupled, Model4.unapply))

  def add(s: Model4) = {
    super.insert(s).execute
  }

  def getById(id: Int) = {
    select(*).filter(this.id == 1).execute.pageResult
  }

  def all() = {
    select(*).execute.pageResult
  }

  def getByGender(gender:Gender)= {
    this.select(*).filter(this.gender == gender).execute.pageResult
  }
}

case class Model5(id:Int,name:String,tags:Set[String],deleted:Boolean,createdAt:Date)

class Model5Table(implicit conn:Connection) extends Table[Model5]("model5") {
  val id = column[Int]("id")
  val name = column[String]("name")
  val tags = setColumn[String]("tags")
  val deleted = column[Boolean]("deleted").index
  val createdAt = column[Date]("created_at")
  override lazy val primaryKey = partitionKeys(id).clusteringKeys(createdAt).orderBy(createdAt Desc)
  val parser =
    (id ~ name ~ tags ~ deleted ~ createdAt <> (Model5.tupled, Model5.unapply))

  def add(s: Model5) = {
    super.insert(s).execute
  }

  def getById(id: Int) = {
    select(*).filter(this.id == 1).execute.pageResult
  }

  def all() = {
    select(*).execute.pageResult
  }

  def getUndeleted = {
    this.select(*).filter(this.deleted == false).execute.pageResult
  }
}

case class Model6(sid:Int,uid:Int,name:String,words:Map[String,Int],deleted:Boolean,createdAt:Date)
class Model6Table(implicit conn:Connection) extends Table[Model6]("model6") {
  val sid = column[Int]("sid")
  val uid = column[Int]("uid")
  val name = column[String]("name")
  val words = mapColumn[String,Int]("words").keyIndex.valueIndex.entryIndex
  val deleted = column[Boolean]("deleted")
  val createdAt = column[Date]("created_at")
  override lazy val primaryKey = partitionKeys(sid).clusteringKeys(uid).orderBy(uid Asc)
  val parser =
    (sid ~ uid ~ name ~ words ~ deleted ~ createdAt <> (Model6.tupled, Model6.unapply))

  def add(s: Model6) = {
    super.insert(s).execute
  }

  def all() = {
    select(*).execute.pageResult
  }

  def getByKey(wordKey:String)={
    select(*).filter(words containsKey wordKey).execute.pageResult
  }

  def getByValue(wordValue:Int) ={
    select(*).filter(words contains wordValue).execute.pageResult
  }

  def getByWord(key:String,value:Int) ={
    select(*).filter(words hasEntry (key,value)).execute.pageResult
  }

  def getById(sid: Int,uid:Int) = {
    select(*).filter(this.sid == sid).filter(this.uid == uid).execute.pageResult
  }


}


