package catrix.model

import com.datastax.driver.core.Row

/**
  * Created by paul on 2017/9/14.
  */
trait ColumnTuple {

}

trait RowParser[M]{
  type TP <: Product
  //type TVP <: Product
  val columns:Product
  def * = columns.productIterator.map(_.asInstanceOf[Column[_]]).toSeq
  def values(m:M):Seq[ColumnValue[_]]
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
    def values(m:M) = {
      val cvs =this.apply(m)
      (ColumnValue[T1](c1,cvs._1)).
        productIterator.toSeq.map(_.asInstanceOf[ColumnValue[_]])
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
    def values(m:M) = {
      val cvs =this.apply(m)
      (ColumnValue[T1](c1,cvs._1),ColumnValue[T2](c2,cvs._2)).
        productIterator.toSeq.map(_.asInstanceOf[ColumnValue[_]])
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
    def values(m:M) = {
      val cvs =this.apply(m)
      (ColumnValue[T1](c1,cvs._1),ColumnValue[T2](c2,cvs._2),ColumnValue[T3](c3,cvs._3)).
        productIterator.toSeq.map(_.asInstanceOf[ColumnValue[_]])
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
      def values(m:M) = {
        val cvs =this.apply(m)
        (ColumnValue[T1](c1,cvs._1),ColumnValue[T2](c2,cvs._2),ColumnValue[T3](c3,cvs._3),ColumnValue[T4](c4,cvs._4)).
          productIterator.toSeq.map(_.asInstanceOf[ColumnValue[_]])
      }
  }
}

case class ColumnTuple5[T1,T2,T3,T4,T5](c1:Column[T1],c2:Column[T2],c3:Column[T3],c4:Column[T4],c5:Column[T5])
  extends ColumnTuple {

  def ~[T6](c6:Column[T6]) = ColumnTuple6(c1,c2,c3,c4,c5,c6)
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
      def values(m:M) = {
        val cvs =this.apply(m)
        (ColumnValue[T1](c1,cvs._1),ColumnValue[T2](c2,cvs._2),ColumnValue[T3](c3,cvs._3),ColumnValue[T4](c4,cvs._4),ColumnValue[T5](c5,cvs._5)).
          productIterator.toSeq.map(_.asInstanceOf[ColumnValue[_]])
      }
    }
}

case class ColumnTuple6[T1,T2,T3,T4,T5,T6](c1:Column[T1],c2:Column[T2],c3:Column[T3],c4:Column[T4],c5:Column[T5],c6:Column[T6])
  extends ColumnTuple {

  def ~[T7](c7:Column[T7]) = ColumnTuple7(c1,c2,c3,c4,c5,c6,c7)
  abstract class RowParser6[M, T1, T2, T3, T4, T5,T6](val columns: ColumnTuple6[T1, T2, T3, T4, T5,T6]) extends RowParser[M] {
    type TP = Tuple6[T1, T2, T3, T4, T5,T6]
    type TVP = Tuple6[ColumnValue[T1], ColumnValue[T2], ColumnValue[T3], ColumnValue[T4], ColumnValue[T5],ColumnValue[T6]]

    def apply(m: M): TP
  }

  def <>[M](f1: Tuple6[T1, T2, T3, T4, T5,T6] => M, f2: M => Option[Tuple6[T1, T2, T3, T4, T5,T6]]): RowParser6[M, T1, T2, T3, T4, T5,T6] =
    new RowParser6[M, T1, T2, T3, T4, T5,T6](this) {
      def parse(row: Row): M = f1(Tuple6(c1(row), c2(row), c3(row), c4(row), c5(row),c6(row)))

      def apply(m: M) = {
        f2(m).get
      }
      def values(m:M) = {
        val cvs =this.apply(m)
        (ColumnValue[T1](c1,cvs._1),ColumnValue[T2](c2,cvs._2),ColumnValue[T3](c3,cvs._3),ColumnValue[T4](c4,cvs._4),ColumnValue[T5](c5,cvs._5),
          ColumnValue[T6](c6,cvs._6)).
          productIterator.toSeq.map(_.asInstanceOf[ColumnValue[_]])
      }
    }
}

case class ColumnTuple7[T1,T2,T3,T4,T5,T6,T7](c1:Column[T1],c2:Column[T2],c3:Column[T3],c4:Column[T4],c5:Column[T5],c6:Column[T6],c7:Column[T7])
  extends ColumnTuple {

  def ~[T8](c8:Column[T8]) = ColumnTuple8(c1,c2,c3,c4,c5,c6,c7,c8)
  abstract class RowParser7[M, T1, T2, T3, T4, T5,T6,T7]
          (val columns: ColumnTuple7[T1, T2, T3, T4, T5,T6,T7]) extends RowParser[M] {
    type TP = Tuple7[T1, T2, T3, T4, T5,T6,T7]
    type TVP = Tuple7[ColumnValue[T1], ColumnValue[T2], ColumnValue[T3], ColumnValue[T4], ColumnValue[T5],ColumnValue[T6],ColumnValue[T7]]

    def apply(m: M): TP
  }

  def <>[M](f1: Tuple7[T1, T2, T3, T4, T5,T6,T7] => M,
            f2: M => Option[Tuple7[T1, T2, T3, T4, T5,T6,T7]]): RowParser7[M, T1, T2, T3, T4, T5,T6,T7] =
    new RowParser7[M, T1, T2, T3, T4, T5,T6,T7](this) {
      def parse(row: Row): M = f1(Tuple7(c1(row), c2(row), c3(row), c4(row), c5(row),c6(row),c7(row)))

      def apply(m: M) = {
        f2(m).get
      }
      def values(m:M) = {
        val cvs =this.apply(m)
        (ColumnValue[T1](c1,cvs._1),ColumnValue[T2](c2,cvs._2),ColumnValue[T3](c3,cvs._3),ColumnValue[T4](c4,cvs._4),ColumnValue[T5](c5,cvs._5),
          ColumnValue[T6](c6,cvs._6),ColumnValue[T7](c7,cvs._7)).
          productIterator.toSeq.map(_.asInstanceOf[ColumnValue[_]])
      }
    }
}

case class ColumnTuple8[T1,T2,T3,T4,T5,T6,T7,T8](c1:Column[T1],c2:Column[T2],c3:Column[T3],c4:Column[T4],c5:Column[T5],c6:Column[T6],c7:Column[T7],c8:Column[T8])
  extends ColumnTuple {

  def ~[T9](c9:Column[T9]) = ColumnTuple9(c1,c2,c3,c4,c5,c6,c7,c8,c9)
  abstract class RowParser8[M, T1, T2, T3, T4, T5,T6,T7,T8]
  (val columns: ColumnTuple8[T1, T2, T3, T4, T5,T6,T7,T8]) extends RowParser[M] {
    type TP = Tuple8[T1, T2, T3, T4, T5,T6,T7,T8]

    def apply(m: M): TP
  }

  def <>[M](f1: Tuple8[T1, T2, T3, T4, T5,T6,T7,T8] => M,
            f2: M => Option[Tuple8[T1, T2, T3, T4, T5,T6,T7,T8]]): RowParser8[M, T1, T2, T3, T4, T5,T6,T7,T8] =
    new RowParser8[M, T1, T2, T3, T4, T5,T6,T7,T8](this) {
      def parse(row: Row): M = f1(Tuple8(c1(row), c2(row), c3(row), c4(row), c5(row),c6(row),c7(row),c8(row)))
      def apply(m: M) = {
        f2(m).get
      }
      def values(m:M) = {
        val cvs =this.apply(m)
        (ColumnValue[T1](c1,cvs._1),ColumnValue[T2](c2,cvs._2),ColumnValue[T3](c3,cvs._3),ColumnValue[T4](c4,cvs._4),ColumnValue[T5](c5,cvs._5),
          ColumnValue[T6](c6,cvs._6),ColumnValue[T7](c7,cvs._7),ColumnValue[T8](c8,cvs._8)).
          productIterator.toSeq.map(_.asInstanceOf[ColumnValue[_]])
      }
    }
}

case class ColumnTuple9[T1,T2,T3,T4,T5,T6,T7,T8,T9](c1:Column[T1],c2:Column[T2],c3:Column[T3],c4:Column[T4],c5:Column[T5],c6:Column[T6],c7:Column[T7],c8:Column[T8],c9:Column[T9])
  extends ColumnTuple {

    //def ~[T1](t:CassandraColumn[T1]) = TupledColumns2(this.
    abstract class RowParser9[M, T1, T2, T3, T4, T5, T6, T7, T8, T9]
    (val columns: ColumnTuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]) extends RowParser[M] {
      type TP = Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]

      def apply(m: M): TP
    }

    def <>[M](f1: Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9] => M,
              f2: M => Option[Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]]): RowParser9[M, T1, T2, T3, T4, T5, T6, T7, T8, T9] =
      new RowParser9[M, T1, T2, T3, T4, T5, T6, T7, T8, T9](this) {
        def parse(row: Row): M = f1(Tuple9(c1(row), c2(row), c3(row), c4(row), c5(row), c6(row), c7(row), c8(row),c9(row)))

        def apply(m: M) = {
          f2(m).get
        }
        def values(m:M) = {
          val cvs =this.apply(m)
          (ColumnValue[T1](c1,cvs._1),ColumnValue[T2](c2,cvs._2),ColumnValue[T3](c3,cvs._3),ColumnValue[T4](c4,cvs._4),ColumnValue[T5](c5,cvs._5),
            ColumnValue[T6](c6,cvs._6),ColumnValue[T7](c7,cvs._7),ColumnValue[T8](c8,cvs._8),ColumnValue[T9](c9,cvs._9)).
            productIterator.toSeq.map(_.asInstanceOf[ColumnValue[_]])
        }
      }

}
