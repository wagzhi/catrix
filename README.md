# catrix
Catrix是一个使用Scala语言封装的的，基于cassandra-driver-core实现的Cassandra数据库访问库。
期实现方式参考了Slick，使用Catrix你可以像使用Scala collections那样访问Cassandra,而不需要写Cql。
另外，通过定义Table，你还可以直接通过Table生成对应的建表Cql语句，提升开发效率。
当然，Cassandra建模和查询非常复杂，在用于真实的产品中，还是需要你对Cassandra数据库和Cql及相关性能优化有深入的了解。

###定义表和访问接口
```scala
case class Coffee(name:String, prices:Int)
class Coffees(implicit conn:Connection) extends Table[Coffee]("coffee") {
    val name = column[String]("name")
    val price = column[Int]("price").index
    val primaryKey = partitionKeys(name).clusteringKeys(price).orderBy(price Asc)
    val parser = (name ~ parser) <> (Coffee.tupled , Coffee.unapply)
    
    def save(coffee:Coffee)=
        insert(coffee).execute
    
    def getByName(name:String):ResultPage[Coffee] =
        select(*).filter(this.name == name).execute.resultPage
    
    //if pagingState is empty string, get first page and next pagingState included in result
    def getPage(pagingState:String,pageSize:Int) =
        select(*).page(pagingState,pageSize).execute.resultPage
}
```


### 获取表结构，或直接生成表
```scala
implicit val conn = Catrix.connect("127.0.0.1","keyspace_name")
val coffees = new Coffees
//打印建表语句，包括索引
coffees.createCqls.foreach(println)
//删除表，dropCql语句包含if exits
conn.session.execute(new SimpleStatement(table.dropCql))
//新建表
coffees.createCqls.
      map(_=>new SimpleStatement(table,_)).
      foreach(conn.session.execute)
```

### 使用表接口访问数据
```scala
implicit val conn = Catrix.connect("127.0.0.1","keyspace_name")
val coffees = new Coffees
val coffee = new Coffees("Blue Mountain Coffee","97")
coffees.save(coffee)
val coffeePage = coffees.getPage("",20)
coffeePage.results.foreach(pringln)
val nextPage = coffees.getPage(coffeePage.pagingState,20)
```



