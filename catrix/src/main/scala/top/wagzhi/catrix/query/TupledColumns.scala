package top.wagzhi.catrix.query

import com.datastax.driver.core.Row

/**
  * Created by paul on 2017/9/13.
  */
trait TupledColumns {
  val columns:Columns
}

abstract class RowParser[M](val columns: Columns){
  type T <: Product
  def parse(row:Row):M
  def apply(m:M):Seq[ColumnValue[_]]
}

case class TupledColumns1[T1](t:CassandraColumn[T1]) extends TupledColumns{
  override val columns = Columns(Seq(t))
  def ~[T1](t:CassandraColumn[T1]) = TupledColumns2(this.t,t)
  def <>[M](f:Tuple1[T1]=>M,f2:M=>Tuple1[T1]):RowParser[M] =new RowParser[M](columns){
    override def parse(row: Row): M = {
      val v = t.apply(row)
      f(Tuple1(v))
    }
    def apply(m:M) ={
      Seq(ColumnValue(t,f2(m)))
    }
  }
}

case class TupledColumns2[T1,T2](t1:CassandraColumn[T1],t2:CassandraColumn[T2]) extends TupledColumns{
  override val columns = Columns(Seq(t1,t2))
  def <>[M](f:Tuple2[T1,T2]=>M,f2:M=>Option[Tuple2[T1,T2]]):RowParser[M] =new RowParser[M](columns) {
    override def parse(row: Row): M =
      f(t1(row), t2(row))

    def apply(m: M) = {
      val vt = f2(m).get
      Seq(ColumnValue(t1, vt._1), ColumnValue(t2, vt._2))
    }
  }

}

object R extends App{
}