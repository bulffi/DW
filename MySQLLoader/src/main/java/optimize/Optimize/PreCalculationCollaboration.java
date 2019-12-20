package optimize.Optimize;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * XXXXXXXXX科技有限公司 版权所有 © Copyright 2018<br>
 *
 * @Description: <br>
 * @Project: hades <br>
 * @CreateDate: Created in 2019/12/19 13:15 <br>
 * @Author: <a href="abc@qq.com">abc</a>
 */
public class PreCalculationCollaboration {
    private static Connection con; //声明 Connection 对象
    private static PreparedStatement pStmt;//声明预处理 PreparedStatement 对象
    private static ResultSet res;//声明结果 ResultSet 对象


    public static void main(String[] args) throws SQLException {
        con = optimize.MySQLLoaderOptimize.getConnection(2);
        //preCalculateActorCollaboration();
        //preCalculateDirectorCollaboration();
    }

    public static void preCalculateActorCollaboration() throws SQLException {//计算合作次数100次及以上的演员
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

        for(int i = 0; i < activeActors.size(); i++){
            for( int j = i + 1; j < activeActors.size(); j++){
                System.out.println("(" + i + ", " + j + ")");
                String actor1 = activeActors.get(i);
                String actor2 = activeActors.get(j);
                pStmt = con.prepareStatement(
                        "select count(temp0.movie_title) from " +
                                "(select movie_title, movie_id from prejoin_movie_actor where actor = ?)as temp0 " +
                                "inner join " +
                                "(select movie_title, movie_id from prejoin_movie_actor where actor = ?)as temp1 " +
                                "on temp0.movie_id = temp1.movie_id");
                pStmt.setString(1, actor1);
                pStmt.setString(2, actor2);
                res = pStmt.executeQuery();
                res.next();
                int collaboration_time = res.getInt(1);
                //if(collaboration_time > 50) {
                    pStmt = con.prepareStatement("insert into precal_collaboration_actor values (?,?,?)");
                    pStmt.setString(1, actor1);
                    pStmt.setString(2, actor2);
                    pStmt.setInt(3, collaboration_time);
                    pStmt.execute();
                    System.out.println("插入成功");
                //}
            }
        }
    }

    public static void preCalculateDirectorCollaboration() throws SQLException {//计算合作次数50次及以上的演员
        pStmt = con.prepareStatement(
                "select director_name " +
                        "from prejoin_movie_director " +
                        "group by director_name " +
                        "having count(*) >= 50");
        res = pStmt.executeQuery();
        List<String> activeDirectors = new ArrayList<>();//执导次数大于等于50次的导演
        while(res.next()){
            activeDirectors.add(res.getString(1));
        }

        for(int i = 0; i < activeDirectors.size(); i++){
            for( int j = i + 1; j < activeDirectors.size(); j++){
                System.out.println("(" + i + ", " + j + ")");
                String director1 = activeDirectors.get(i);
                String director2 = activeDirectors.get(j);
                pStmt = con.prepareStatement(
                        "select count(temp0.movie_title) from " +
                                "(select movie_title, movie_id from prejoin_movie_director where director_name = ?)as temp0 " +
                                "inner join " +
                                "(select movie_title, movie_id from prejoin_movie_director where director_name = ?)as temp1 " +
                                "on temp0.movie_id = temp1.movie_id");
                pStmt.setString(1, director1);
                pStmt.setString(2, director2);
                res = pStmt.executeQuery();
                res.next();
                int collaboration_time = res.getInt(1);
                pStmt = con.prepareStatement("insert into precal_collaboration_director values (?,?,?)");
                pStmt.setString(1, director1);
                pStmt.setString(2, director2);
                pStmt.setInt(3, collaboration_time);
                pStmt.execute();
                //System.out.println("插入成功");
            }
        }
    }

    public static void preCalculateActorDirectorCollaboration() throws SQLException {//计算合作次数100次及以上的演员和导演
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
