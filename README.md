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
