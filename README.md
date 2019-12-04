# Data warehouse
The course project for data warehouse. It consists of several parts as follows:

- pre-processor (python [textblob](https://textblob.readthedocs.io/en/dev/quickstart.html#textblobs-are-like-python-strings))
- web crawler (python [scrapy](https://scrapy.org/))
- instance matching (java [hadoop](https://hadoop.apache.org/))
- relational, graph and distributed DB injector (java)
- query server (java [spring boot](https://spring.io/))
- web controller (javascript [vue](https://vuejs.org/))

## 前期数据

前期数据在 /data 文件夹中，对其中文件对简要说明如下：
1.  DDL.sql （ SQL Server 中两张表的定义，只能用作参考，不能直接运行在其他的数据库上。
2.  loadReviewIntoADataBase.py （将从网站上下载的那个几个 G 的文件导入数据库的脚本，前半段可以直接用，也就是提取字段的那部分，后面要根据自己用的数据库来写）
3.  movieInfo.csv （从亚马逊爬的原始数据，是 Instance Matching 模块的原始输入）
4.  part-r-00000(1) （经过 Instance Matching 之后的文件，每一行是一个 json，可以用 FastJson 解析成对象，对象的定义在 InstanceMatching 模块中 Entity 包下的 MovieInfo.java。一个简单的例子如下
``` java
BufferedReader reader = new BufferedReader(new FileReader("/Users/zhangzijian/Downloads/DW/part-r-00000(1)"));
    for (int i = 0; i < 100; i++) {
      String line = reader.readLine();
      Movie movie = JSON.parseObject(line,Movie.class);
      loader.addMovieAlongWithDirectorAndActor(movie);
    } 
```

## Query Server 模版说明

在完成了数据的导入之后，需要完成数据的查询工作，我们定义了统一的模版从而简化工作。简要说明如下：

![query](img/query.png)

我们可以清楚的看到，有四个字模块，分别是

- entity （存放问题与答案，都是实体类，具有标准的 getter 和 setter 函数，用于实例化前端对象并向前端返回对象）
- mysql
- hbase
- neo4j

其中，后三个模块就是各个自己的查询，他们的主函数（也就是获得前端数据以及返回给前端的地方就在 ***MainHandler 类中，每个类的定义都大致如下所示

```java
@RestController
public class HBaseMainHandler {
    @PostMapping("/HBase/specify")
    public Object handleSpecifyQuery(@RequestBody SpecificQuery specificQuery){
        return new DataAnswer();
    }
    @PostMapping("/HBase/general/collaboration")
    public Object handleCollaborationQuery(@RequestBody CollaborateQuery collaborateQuery){
        return  new DataAnswer();
    }
}
```

可以看到，你将可以在函数体中获得整个 query 对象，根据要求，你将会返回 DataAnswer（询问具体数据，顺便返回时间） 或者是 TestAnswer（获取查询时间）。