package top.wagzhi.catrix

/**
  * Created by paul on 2017/7/20.
  */
trait BaseModel {
  def unapply(): Seq[Any]
}
