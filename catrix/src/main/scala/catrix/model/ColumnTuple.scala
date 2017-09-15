package catrix.model

import com.datastax.driver.core.Row

/**
  * Created by paul on 2017/9/14.
  */
trait ColumnTuple {

}

trait RowParser[M]{
  type TP <: Product
  type TVP <: Product
  val columns:Product
  def parse(row:Row):M
  def apply(m:M):TVP
}
case class ColumnTuple1[T1](c1:Column[T1]) extends ColumnTuple{
  def ~[T2](c2:Column[T2]):ColumnTuple2[T1,T2] = ColumnTuple2[T1,T2](c1,c2)

  abstract class RowParser1[M,T1] (val columns:ColumnTuple1[T1]) extends RowParser[M]{
    type TP = Tuple1[T1]
    type TVP = Tuple1[ColumnValue[T1]]
  }
  def <>[M](f:Tuple1[T1]=>M,f2:M=>Option[Tuple1[T1]]):RowParser1[M,T1] = new RowParser1[M,T1](this) {

    def parse(row:Row):M = f(Tuple1(c1(row)))
    def apply(m:M):TVP = {
      val cvs = f2(m).get
      Tuple1(ColumnValue(c1,cvs._1))
    }
  }
}
abstract class RowParser2[M,T1,T2] (val columns:ColumnTuple2[T1,T2]) extends RowParser[M]{
  type TP = Tuple2[T1,T2]
  type TVP = Tuple2[ColumnValue[T1],ColumnValue[T2]]
  def apply(m:M):Tuple2[ColumnValue[T1],ColumnValue[T2]]
}

case class ColumnTuple2[T1,T2](c1:Column[T1],c2:Column[T2]) extends ColumnTuple{
//  def ~[T3](c3:Column[T3]) = ColumnTuple3(c1,c2,c3)
  def <>[M](f1:Tuple2[T1,T2]=>M,f2:M=>Option[(T1,T2)]):RowParser2[M,T1,T2]  = new RowParser2[M,T1,T2](this) {

    def parse(row:Row):M = {
      f1(Tuple2(c1(row),c2(row)))
    }
    def apply(m:M):Tuple2[ColumnValue[T1],ColumnValue[T2]] = {
      val cvs = f2(m).get
      Tuple2(ColumnValue(c1,cvs._1),ColumnValue(c2,cvs._2))
    }
  }
}

//case class ColumnTuple3[T1,T2,T3](c1:Column[T1],c2:Column[T2],c3:Column[T3]) extends ColumnTuple {
//
//  //def ~[T1](t:CassandraColumn[T1]) = TupledColumns2(this.
//  abstract class RowParser3[M, T1, T2, T3](val columns: ColumnTuple3[T1, T2, T3]) extends RowParser[M] {}
//  def <>[M](f1: Tuple3[T1, T2, T3] => M, f2: M => Tuple3[T1, T2, T3]):RowParser3[M,T1,T2,T3]  = new RowParser3[M, T1, T2, T3](this) {
//    def parse(row: Row): M = f1(Tuple3(c1(row), c2(row), c3(row)))
//    def toColumns(m: M) = {
//      val cvs = f2(m)
//      Tuple3(ColumnValue(c1, cvs._1), ColumnValue(c2, cvs._2), ColumnValue(c3, cvs._3))
//    }
//  }
//}
