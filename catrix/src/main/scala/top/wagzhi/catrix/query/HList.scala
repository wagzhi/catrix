package top.wagzhi.catrix.query


/**
  * Created by paul on 2017/9/13.
  */
sealed trait HList

final case class HCons[H, T <: HList](head : H, tail : T) extends HList {
  def ::[T](v : T) = HCons(v, this)

  
}

case class HNil() extends HList {
  def ::[T](v : T) = HCons(v, this)
}

// aliases for building HList types and for pattern matching
object HList {
  type ::[H, T <: HList] = HCons[H,T]
  val :: = HCons
}
case class User(id:Int,name:String)

case class Result[T](v:T)
object Run extends App{
  type ::[A,B <: HList] = HCons[A,B]
  val x = "str" :: true :: 1 :: HNil()
  val  f: (String::Boolean::Int::HNil) => User  ={
    v=>
      User(v.tail.tail.head,v.head)
  }
  println(f(x))
}

