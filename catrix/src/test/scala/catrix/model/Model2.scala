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



