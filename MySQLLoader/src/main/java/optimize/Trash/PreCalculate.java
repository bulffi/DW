package optimize.Trash;

import optimize.MySQLLoaderOptimize;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * XXXXXXXXX科技有限公司 版权所有 © Copyright 2018<br>
 *
 * @Description: <br>
 * @Project: hades <br>
 * @CreateDate: Created in 2019/12/18 11:01 <br>
 * @Author: <a href="abc@qq.com">abc</a>
 */
public class PreCalculate {
    private static Connection con; //声明 Connection 对象
    static public void main(String[] args) throws SQLException {
        //建立数据库连接
        con = MySQLLoaderOptimize.getConnection(2);
        Query1_Aggregation();
    }

    public static void Query1_Aggregation() throws SQLException {
//        create table Query1
//        (
//            year integer,
//            month integer,
//            date integer,
//            weekday integer,
//            year_quarter integer,
//            year_half integer,
//            count integer
//        );
        PreparedStatement preparedStatement;
        ResultSet resultSet;


        //某某年有多少电影
        preparedStatement = con.prepareStatement(
                "select year, count(*) " +
                        "from movie_info_fact " +
                        "group by year");
        resultSet = preparedStatement.executeQuery();
        preparedStatement = con.prepareStatement(
                "insert into Query1 " +
                        "values (?,0,0,0,0,0,?)");
        while(resultSet.next()){
            int year = resultSet.getInt(1);
            if(year != 0) {
                preparedStatement.setInt(1, resultSet.getInt(1));
                preparedStatement.setInt(2, resultSet.getInt(2));
                preparedStatement.execute();
            }
        }

        //某某月有多少电影
        preparedStatement = con.prepareStatement(
                "select month, count(*) " +
                        "from movie_info_fact natural join movie_release_date " +
                        "group by month");
        resultSet = preparedStatement.executeQuery();
        preparedStatement = con.prepareStatement(
                "insert into Query1 " +
                        "values (0,?,0,0,0,0,?)");
        while(resultSet.next()){
            int month = resultSet.getInt(1);
            if(month != 0) {
                preparedStatement.setInt(1, month);
                preparedStatement.setInt(2, resultSet.getInt(2));
                preparedStatement.execute();
            }
        }

        //某某年某某月有多少电影
        preparedStatement = con.prepareStatement(
                "select year, month, count(*) " +
                        "from movie_info_fact natural join movie_release_date " +
                        "group by (year, month)");
        resultSet = preparedStatement.executeQuery();
        preparedStatement = con.prepareStatement(
                "insert into Query1 " +
                        "values (?,?,0,0,0,0,?)");
        while(resultSet.next()){
            int year = resultSet.getInt(1);
            int month = resultSet.getInt(2);
            int count = resultSet.getInt(3);
            if(year != 0 && month != 0){
                preparedStatement.setInt(1, year);
                preparedStatement.setInt(2, month);
                preparedStatement.setInt(3, count);
                preparedStatement.execute();
            }
        }

        //某某年某某季度上映多少电影
        preparedStatement = con.prepareStatement(
                "select year, year_quarter, count(*) " +
                        "from movie_info_fact natural join movie_release_date " +
                        "group by (year, year_quarter)");
        resultSet = preparedStatement.executeQuery();
        preparedStatement = con.prepareStatement(
                "insert into Query1 " +
                        "values (?,0,0,0,?,0,?)");
        while(resultSet.next()){
            int year = resultSet.getInt(1);
            int year_quarter = resultSet.getInt(2);
            int count = resultSet.getInt(3);
            if(year != 0 && year_quarter != 0){
                preparedStatement.setInt(1, year);
                preparedStatement.setInt(2, year_quarter);
                preparedStatement.setInt(3, count);
                preparedStatement.execute();
            }
        }

        //星期某上映多少电影
        preparedStatement = con.prepareStatement(
                "select weekday, count(*) " +
                        "from movie_info_fact " +
                        "group by weekday");
        resultSet = preparedStatement.executeQuery();
        preparedStatement = con.prepareStatement(
                "insert into Query1 " +
                        "values (0,0,0,?,0,0,?)");
        while(resultSet.next()){
            Object o = resultSet.getObject(1);
            if(o != null){
                int weekday = resultSet.getInt(1);
                if(weekday == 0)
                    preparedStatement.setInt(1, 7);
                else
                    preparedStatement.setInt(1, weekday);
                preparedStatement.setInt(2, resultSet.getInt(2));
                preparedStatement.execute();
            }
        }
    }
}
