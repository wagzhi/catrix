package catrix

import java.nio.ByteBuffer
import java.util.Date

import catrix.model.{Model8, Model8Table}

/**
  * Created by paul on 2017/9/21.
  */
class Model8Test extends ModelTest[Model8] {

  val now = System.currentTimeMillis()
  override val samples: Seq[Model8] = Seq(
    Model8(1,1, "张三",Seq(1,2,3),Array[Byte](1,2,3),Some("abc"),true,new Date(now)),
    Model8(1,2, "李四",Seq(2,3,4),Array[Byte](1,2,3),None,true,new Date(now-100)),
    Model8(1,3, "王五",Seq(4,5,6,7,7),Array[Byte](1,2,3,6,6,7),Some("很好"),true,new Date(now-200)),
    Model8(1,4, "六六",Seq(),Array[Byte](1,2,3),None,true,new Date(now-300))
  )

  override def table(implicit conn: Connection) = new Model8Table()

  "model8" should "get all" in {
    f =>
      val table = f.table.asInstanceOf[Model8Table]
      samples.map(table.add)
      val models = table.all.results
      for(i <- 0 to 3){
        val m = models(i)
        val s = samples(i)
        m.image shouldBe s.image
        m.copy(image = s.image) shouldBe s
      }
  }
  it should "get by equals" in {
    f=>
      val table = f.table.asInstanceOf[Model8Table]
      samples.map(table.add)
      val m = table.getByContent("很好").results.head

      //Array[Byte] with same content ,but not same object is not equal in case class
      m.image shouldBe samples(2).image
      m.copy(image=samples(2).image) shouldBe samples(2)
  }

}

