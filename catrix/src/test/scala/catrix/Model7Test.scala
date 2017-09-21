package catrix

import java.nio.ByteBuffer
import java.util.Date

import catrix.model.{Model7, Model7Table}

/**
  * Created by paul on 2017/9/21.
  */
class Model7Test extends ModelTest[Model7] {

  val now = System.currentTimeMillis()
  override val samples: Seq[Model7] = Seq(
    Model7(1,1, "张三",Seq(1,2,3),ByteBuffer.wrap(Array[Byte](1,2,3)),true,new Date(now)),
    Model7(1,2, "李四",Seq(2,3,4),ByteBuffer.wrap(Array[Byte](1,2,3)),true,new Date(now-100)),
    Model7(1,3, "王五",Seq(4,5,6,7,7),ByteBuffer.wrap(Array[Byte](1,2,3,6,6,7)),true,new Date(now-200)),
    Model7(1,4, "六六",Seq(),ByteBuffer.wrap(Array[Byte](1,2,3)),true,new Date(now-300))
  )

  override def table(implicit conn: Connection) = new Model7Table()

  "model7" should "get all" in {
    f =>
      val table = f.table.asInstanceOf[Model7Table]
      samples.map(table.add)
      val models = table.all.results
      models shouldBe samples
  }

}

