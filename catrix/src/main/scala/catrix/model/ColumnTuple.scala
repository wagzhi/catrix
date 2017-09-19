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
  def * = columns.productIterator.map(_.asInstanceOf[Column[_]]).toSeq
  def parse(row:Row):M
  def apply(m:M):TP
}
case class ColumnTuple1[T1](c1:Column[T1]) extends ColumnTuple{
  def ~[T2](c2:Column[T2]):ColumnTuple2[T1,T2] = ColumnTuple2[T1,T2](c1,c2)

  abstract class RowParser1[M,T1] (val columns:ColumnTuple1[T1]) extends RowParser[M]{
    type TP = Tuple1[T1]
    type TVP = Tuple1[ColumnValue[T1]]
    def apply(m:M):TP
  }
  def <>[M](f:Tuple1[T1]=>M,f2:M=>Option[Tuple1[T1]]):RowParser1[M,T1] = new RowParser1[M,T1](this) {
    def parse(row:Row):M = f(Tuple1(c1(row)))
    def apply(m:M):TP = {
      val cvs = f2(m).get
      Tuple1(cvs._1)
    }
  }
}
abstract class RowParser2[M,T1,T2] (val columns:ColumnTuple2[T1,T2]) extends RowParser[M]{
  type TP = Tuple2[T1,T2]
  type TVP = Tuple2[ColumnValue[T1],ColumnValue[T2]]
  def apply(m:M):TP
}

case class ColumnTuple2[T1,T2](c1:Column[T1],c2:Column[T2]) extends ColumnTuple{
  def ~[T3](c3:Column[T3]) = ColumnTuple3(c1,c2,c3)
  def <>[M](f1:Tuple2[T1,T2]=>M,f2:M=>Option[(T1,T2)]):RowParser2[M,T1,T2]  = new RowParser2[M,T1,T2](this) {
    def parse(row:Row):M = {
      f1(Tuple2(c1(row),c2(row)))
    }
    def apply(m:M):Tuple2[T1,T2] = {
      val cvs = f2(m).get
      Tuple2(cvs._1,cvs._2)
    }
  }
}

case class ColumnTuple3[T1,T2,T3](c1:Column[T1],c2:Column[T2],c3:Column[T3]) extends ColumnTuple {

  def ~[T4](c4:Column[T4]) = ColumnTuple4(c1,c2,c3,c4)
  abstract class RowParser3[M, T1, T2, T3](val columns: ColumnTuple3[T1, T2, T3]) extends RowParser[M] {
    type TP = Tuple3[T1,T2,T3]
    type TVP = Tuple3[ColumnValue[T1],ColumnValue[T2],ColumnValue[T3]]
    def apply(m:M):TP
  }
  def <>[M](f1: Tuple3[T1, T2, T3] => M, f2: M => Option[Tuple3[T1, T2, T3]]):RowParser3[M,T1,T2,T3]  = new RowParser3[M, T1, T2, T3](this) {
    def parse(row: Row): M = f1(Tuple3(c1(row), c2(row), c3(row)))
    def apply(m: M) = {
      val cvs = f2(m).get
      Tuple3(cvs._1,cvs._2, cvs._3)
    }
  }
}


case class ColumnTuple4[T1,T2,T3,T4](c1:Column[T1],c2:Column[T2],c3:Column[T3],c4:Column[T4]) extends ColumnTuple {

  def ~[T5](c5:Column[T5]) = ColumnTuple5(c1,c2,c3,c4,c5)
  abstract class RowParser4[M, T1, T2, T3,T4](val columns: ColumnTuple4[T1, T2, T3,T4]) extends RowParser[M] {
    type TP = Tuple4[T1,T2,T3,T4]
    type TVP = Tuple4[ColumnValue[T1],ColumnValue[T2],ColumnValue[T3],ColumnValue[T4]]
    def apply(m:M):TP
  }
  def <>[M](f1: Tuple4[T1, T2, T3,T4] => M, f2: M => Option[Tuple4[T1, T2, T3,T4]]):RowParser4[M,T1,T2,T3,T4]  =
    new RowParser4[M, T1, T2, T3,T4](this) {
    def parse(row: Row): M = f1(Tuple4(c1(row), c2(row), c3(row),c4(row)))
    def apply(m: M) = {
      val cvs = f2(m).get
      Tuple4(cvs._1,cvs._2, cvs._3,cvs._4)
    }
  }
}

case class ColumnTuple5[T1,T2,T3,T4,T5](c1:Column[T1],c2:Column[T2],c3:Column[T3],c4:Column[T4],c5:Column[T5])
  extends ColumnTuple {

  //def ~[T1](t:CassandraColumn[T1]) = TupledColumns2(this.
  abstract class RowParser5[M, T1, T2, T3, T4, T5](val columns: ColumnTuple5[T1, T2, T3, T4, T5]) extends RowParser[M] {
    type TP = Tuple5[T1, T2, T3, T4, T5]
    type TVP = Tuple5[ColumnValue[T1], ColumnValue[T2], ColumnValue[T3], ColumnValue[T4], ColumnValue[T5]]

    def apply(m: M): TP
  }

  def <>[M](f1: Tuple5[T1, T2, T3, T4, T5] => M, f2: M => Option[Tuple5[T1, T2, T3, T4, T5]]): RowParser5[M, T1, T2, T3, T4, T5] =
    new RowParser5[M, T1, T2, T3, T4, T5](this) {
      def parse(row: Row): M = f1(Tuple5(c1(row), c2(row), c3(row), c4(row), c5(row)))

      def apply(m: M) = {
        val cvs = f2(m).get
        Tuple5(cvs._1, cvs._2, cvs._3, cvs._4, cvs._5)
      }
    }
}
