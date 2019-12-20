package optimize.Optimize;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * XXXXXXXXX科技有限公司 版权所有 © Copyright 2018<br>
 *
 * @Description: <br>
 * @Project: hades <br>
 * @CreateDate: Created in 2019/12/19 14:22 <br>
 * @Author: <a href="abc@qq.com">abc</a>
 */
public class TempThread2 {
    private static Connection con; //声明 Connection 对象
    private static PreparedStatement pStmt;//声明预处理 PreparedStatement 对象
    private static ResultSet res;//声明结果 ResultSet 对象
    public static Connection getConnection(int host) {
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
    public static void main(String[] args) throws SQLException {
        con = getConnection(2);
        //初始化导演
        pStmt = con.prepareStatement(
                "select director_name " +
                        "from prejoin_movie_director " +
                        "group by director_name " +
                        "having count(*) >= 100");
        res = pStmt.executeQuery();
        List<String> activeDirectors = new ArrayList<>();//执导次数大于等于100次的导演
        while(res.next()){
            activeDirectors.add(res.getString(1));
        }

        //初始化演员
        pStmt = con.prepareStatement(
                "select actor " +
                        "from prejoin_movie_actor " +
                        "group by actor " +
                        "having count(*) >= 100");
        res = pStmt.executeQuery();
        List<String> activeActors = new ArrayList<>();//参演次数大于等于100次的演员
        while(res.next()){
            activeActors.add(res.getString(1));
        }


        for(int i = 0; i < activeDirectors.size(); i++){
            for( int j = 0; j < activeActors.size(); j++){
                System.out.println("(" + i + ", " + j + ")");
                String director = activeDirectors.get(i);
                String actor = activeActors.get(j);
                pStmt = con.prepareStatement(
                        "select count(temp0.movie_title) from " +
                                "(select movie_title, movie_id from prejoin_movie_director where director_name = ?)as temp0 " +
                                "inner join " +
                                "(select movie_title, movie_id from prejoin_movie_actor where actor = ?)as temp1 " +
                                "on temp0.movie_id = temp1.movie_id");
                pStmt.setString(1, director);
                pStmt.setString(2, actor);
                res = pStmt.executeQuery();
                res.next();
                int collaboration_time = res.getInt(1);
                pStmt = con.prepareStatement("insert into precal_collaboration_director_actor values (?,?,?)");
                pStmt.setString(1, director);
                pStmt.setString(2, actor);
                pStmt.setInt(3, collaboration_time);
                pStmt.execute();
                //System.out.println("插入成功");
            }
        }
    }
}
