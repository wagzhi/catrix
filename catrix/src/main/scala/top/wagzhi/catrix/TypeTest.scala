package top.wagzhi.catrix

import scala.reflect.{ClassTag}
import scala.reflect.runtime.{universe => ru}
/**
  * Created by paul on 2017/8/2.
  */
object TypeTest extends App{
  val a = A[Set[String]]("a")

  def c[T](implicit classTag:ClassTag[T])={
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val cs = m.classSymbol(classTag.runtimeClass)
    cs.typeParams.foreach{
      t:ru.Symbol=>
        println(t.name)
        println(t.isType)
        println(t.asType)
    }

    println(classTag)
  }
  def t[T](implicit typeTag:ru.TypeTag[T])={
    println(typeTag)
    println(typeTag.tpe)
    println(typeTag.tpe.typeArgs)
    println(typeTag.tpe.typeSymbol)
  }

  t[Set[Int]]

}
case class A[T](t:String)