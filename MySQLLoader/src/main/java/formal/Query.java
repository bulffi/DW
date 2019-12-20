package formal;

import java.sql.*;

public class Query {
    public Connection connection;
    public static void main(String[] args) throws SQLException {
        Query query = new Query();

        //代码块（1）：加载数据库驱动类
        try {
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("数据库驱动加载成功");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        //代码块（2）：通过访问数据库的URL获取数据库连接对象
        try {
            // 协议：子协议：//目标IP地址：端口/数据库  在这里test1是之前创建的数据库名
            String url = "jdbc:mysql://localhost:3306/mysql";
            String user = "root";
            String password = "2529428523";
            query.connection = DriverManager.getConnection(url, user, password);
            System.out.println("数据库连接成功");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //执行查询
        System.out.println(query.queryMovieByYear(2010));
//        for(int i = 1; i <= 12; i++)
//            System.out.println(query.queryMovieByYearAndMonth(2010,i));
//        for(int i = 1; i <= 4; i++)
//            System.out.println(query.queryMovieByQuarter(2010,i));
//        for(int i = 0; i <= 6; i++)
//            System.out.println(query.queryMovieByWeekday(i));
//        query.fun1();
//        System.out.println(query.queryMovieByDirector("Various"));
//        System.out.println(query.queryActorByActor("Various",false));
    }

    //xx年有多少电影
    public int queryMovieByYear(int year) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(
                "select count(*) from movie_info_fact where year=?");
        preparedStatement.setInt(1,year);
        long startTime =  System.currentTimeMillis();//开始计时
        ResultSet resultSet = preparedStatement.executeQuery();
        long endTime =  System.currentTimeMillis();//结束计时
        double usedTime = (endTime-startTime)/1000.0;//计算耗时
        System.out.println("一共用时"+usedTime+"秒");//输出耗时
        resultSet.next();
        return resultSet.getInt(1);
    }

    //xx年xx月有多少电影
    public int queryMovieByYearAndMonth(int year, int month) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(
                "select count(*) " +
                        "from movie_info_fact natural join movie_release_date " +
                        "where year=? and month=?");
        preparedStatement.setInt(1,year);
        preparedStatement.setInt(2,month);
        long startTime =  System.currentTimeMillis();//开始计时
        ResultSet resultSet = preparedStatement.executeQuery();
        long endTime =  System.currentTimeMillis();//结束计时
        double usedTime = (endTime-startTime)/1000.0;//计算耗时
        System.out.println("一共用时"+usedTime+"秒");//输出耗时
        resultSet.next();
        return resultSet.getInt(1);
    }

    //xx年xx季度有多少电影
    public int queryMovieByQuarter(int year, int quarter) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(
                "select count(*) " +
                        "from movie_info_fact natural join movie_release_date " +
                        "where year=? and year_quarter=?");
        preparedStatement.setInt(1,year);
        preparedStatement.setInt(2,quarter);
        long startTime =  System.currentTimeMillis();//开始计时
        ResultSet resultSet = preparedStatement.executeQuery();
        long endTime =  System.currentTimeMillis();//结束计时
        double usedTime = (endTime-startTime)/1000.0;//计算耗时
        System.out.println("一共用时"+usedTime+"秒");//输出耗时
        resultSet.next();
        return resultSet.getInt(1);
    }

    //周x新增多少电影（周天为0，其余不变）
    public int queryMovieByWeekday(int weekday) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(
                "select count(*)" +
                        "from movie_info_fact " +
                        "where weekday = ?");
        preparedStatement.setInt(1,weekday);
        long startTime =  System.currentTimeMillis();//开始计时
        ResultSet resultSet = preparedStatement.executeQuery();
        long endTime =  System.currentTimeMillis();//结束计时
        double usedTime = (endTime-startTime)/1000.0;//计算耗时
        System.out.println("一共用时"+usedTime+"秒");//输出耗时
        resultSet.next();
        return resultSet.getInt(1);
    }

    //xx电影有多少版本
    //public int queryVersion()

    //xx导演共有多少电影
    public int queryMovieByDirector(String director) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(
                "select count(*)" +
                        "from movie_director " +
                        "where director_name = ?");
        preparedStatement.setString(1, director);
        long startTime =  System.currentTimeMillis();//开始计时
        ResultSet resultSet = preparedStatement.executeQuery();
        long endTime =  System.currentTimeMillis();//结束计时
        double usedTime = (endTime-startTime)/1000.0;//计算耗时
        System.out.println("一共用时"+usedTime+"秒");//输出耗时
        resultSet.next();
        return resultSet.getInt(1);
    }

    //xx演员主演/参演多少部电影
    public int queryActorByActor(String actor, boolean is_lead) throws SQLException {
        PreparedStatement preparedStatement;
        if(!is_lead) {//参演
            preparedStatement = connection.prepareStatement(
                    "select count(*)" +
                            "from movie_actor " +
                            "where actor = ?");
        }
        else{//主演
            preparedStatement = connection.prepareStatement(
                    "select count(*)" +
                            "from movie_actor " +
                            "where actor = ? and is_lead = true");
        }
        preparedStatement.setString(1, actor);
        long startTime =  System.currentTimeMillis();//开始计时
        ResultSet resultSet = preparedStatement.executeQuery();
        long endTime =  System.currentTimeMillis();//结束计时
        double usedTime = (endTime-startTime)/1000.0;//计算耗时
        System.out.println("一共用时"+usedTime+"秒");//输出耗时
        resultSet.next();
        return resultSet.getInt(1);
    }

    public void fun1() throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(
                "select count(*)" +
                        "from movie_info_fact " +
                        "where weekday = ?");
        preparedStatement.setInt(1,2);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        int num = resultSet.getInt(1);

        preparedStatement = connection.prepareStatement(
                "select movie_id " +
                        "from movie_info_fact " +
                        "where weekday = ?");
        preparedStatement.setInt(1,2);
        resultSet = preparedStatement.executeQuery();
//        for(int i = 1; i <= 5000; i++)
//            resultSet.next();
//        for(int i = 1; i <= 100; i++) {
//            resultSet.next();
//            System.out.println(resultSet.getString(1));
//        }
        for(int i = 1; i <= num - 100; i++)
            resultSet.next();
        for(int i = 1; i <= 100; i++){
            resultSet.next();
            System.out.println(resultSet.getString(1));
        }
    }
}
