package raw;

import com.alibaba.fastjson.JSON;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Scanner;

/**
 * @program: DW
 * @description:
 * @author: Zijian Zhang
 * @create: 2019/11/28
 **/
public class MySQLLoaderRaw {
    private static Connection con; //声明 Connection 对象
    private static PreparedStatement pStmt;//声明预处理 PreparedStatement 对象
    private static ResultSet res;//声明结果 ResultSet 对象

    //建立返回值为 Connection 的方法，host=1表示localhost，host=2表示阿里云主机
    public Connection getConnection(int host) {

        //代码块（1）：加载数据库驱动类
        try {
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("数据库驱动加载成功");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        //代码块（2）：通过访问数据库的URL获取数据库连接对象
        if(host == 1) {
            try {
                // 协议：子协议：//目标IP地址：端口/数据库  在这里test1是之前创建的数据库名
                String url = "jdbc:mysql://localhost:3306/mysql";
                String user = "root";
                String password = "2529428523";
                con = DriverManager.getConnection(url, user, password);
                System.out.println("成功连接localhost:3306/mysql数据库");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        else if(host == 2){
            try {
                // 协议：子协议：//目标IP地址：端口/数据库  在这里test1是之前创建的数据库名
                String url = "jdbc:mysql://am-bp1fhqofzb198v9f990650o.ads.aliyuncs.com:3306/datawarehouse";
                String user = "dw";
                String password = "Zzj6p@saturn@lym@tyl";
                con = DriverManager.getConnection(url, user, password);
                System.out.println("成功连接am-bp1fhqofzb198v9f990650o.ads.aliyuncs.com:3306/datawarehouse数据库");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return con;
    }

    public static void main(String[] args) throws IOException, SQLException {//主方法
        MySQLLoaderRaw h = new MySQLLoaderRaw();//创建本类对象
        con = h.getConnection(2);//与数据库建立连接

        addData();
    }


    //将movieInfo添加到数据库
    public static void addData() throws IOException, SQLException {
        //先删除表中所有的内容
        try {
            Statement stmt = con.createStatement();//创建Statement对象
            stmt.executeUpdate("delete from movie_info");
        } catch (Exception e) {
            e.printStackTrace();
        }
        //将json文件解析成Movie对象
        BufferedReader reader = new BufferedReader(new FileReader("D:\\DataWarehouse\\code\\DW\\Data\\part-r-00000(1)"));
//        for(int i = 0; i < 6; i++) {
        String line;
        line = reader.readLine();
        int i = 1;
        pStmt = con.prepareStatement("insert into movie_info values(?, ?, ?, ?, ?, ?, ?)");
        while(line!=null) {
            try {
                //读取一个json对象
                System.out.println("Reading Object "+i);
                Movie movie = JSON.parseObject(line, Movie.class);

                //添加至数据库
                if(movie.getId().isEmpty())
                    pStmt.setNull(1,Types.VARCHAR);//varchar
                else
                    pStmt.setString(1,movie.getId().toString());
                if(movie.getTitle()==null)
                    pStmt.setNull(2,Types.VARCHAR);
                else
                    pStmt.setString(2,movie.getTitle());
                if(movie.getDirector().isEmpty())
                    pStmt.setNull(3,Types.VARCHAR);
                else
                    pStmt.setString(3,movie.getDirector().toString());
                if(movie.getReleaseDate()==null)
                    pStmt.setNull(4,Types.VARCHAR);
                else
                    pStmt.setString(4,movie.getReleaseDate());
                if(movie.getType().isEmpty())
                    pStmt.setNull(5,Types.VARCHAR);
                else
                    pStmt.setString(5,movie.getType().toString());
                if(movie.getVersion().isEmpty())
                    pStmt.setNull(6,Types.VARCHAR);
                else
                    pStmt.setString(6,movie.getVersion().toString());
                if(movie.getActor().isEmpty())
                    pStmt.setNull(7,Types.VARCHAR);
                else
                    pStmt.setString(7,movie.getActor().toString());

                pStmt.addBatch();
            } catch (Exception e) {
                e.printStackTrace();
            }

            //每1000个提交一次
            if(i % 1000 == 0){
                System.out.println("--------------submit--------------");
                try {
                    pStmt.executeBatch();
                    con.commit();
                    pStmt.clearBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            line = reader.readLine();
            i++;
        }
        try {
            //处理剩下一小部分
            pStmt.executeBatch();
            con.commit();
            pStmt.clearBatch();
            //con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
