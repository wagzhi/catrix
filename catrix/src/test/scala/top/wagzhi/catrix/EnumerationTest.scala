package top.wagzhi.catrix

import top.wagzhi.catrix.Numbers.Numbers

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
/**
  * Created by paul on 2017/9/18.
  */


object Numbers extends Enumeration{
  type Numbers = Value
  val Zero,One,Two,Three = Value
}
object EnumerationTest extends App{
  import Numbers._
 def init[T<:Enumeration#Value](id:Int)(implicit  typeTag:ru.TypeTag[T],  classTag: ClassTag[T]) = {
   lazy val universeMirror = ru.runtimeMirror(getClass.getClassLoader)
   val valueName = typeTag.tpe.toString
   val className = valueName.substring(0,valueName.lastIndexOf('.'))
   val clazz =Class.forName(className+"$")
   val clazz1 = Class.forName(className)
   val cs1 = universeMirror.classSymbol(clazz1)
   val cs = universeMirror.classSymbol(clazz)
   println(cs1.companion)
   //cs1.companion
   val cm =universeMirror.reflectModule(cs1.companion.asModule)
   cm.instance.asInstanceOf[Enumeration].apply(id)
 }
  val values = Seq(0,1,2,3).map(init[Numbers])
  values.foreach(println)
  values.foreach{
    v=>
      v match {
        case Zero=> println(v)
        case One => println(v)
        case Two=> println(v)
        case Three => println(v)
        case _=>throw new Exception("unknow value")
      }
  }

}


