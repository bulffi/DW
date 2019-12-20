package optimize;

import com.alibaba.fastjson.JSON;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

public class MySQLLoaderOptimize {
    private static Connection con; //声明 Connection 对象


    private static PreparedStatement pStmt;//声明预处理 PreparedStatement 对象
    private static ResultSet res;//声明结果 ResultSet 对象

    //建立返回值为 Connection 的方法，host=1表示localhost，host=2表示阿里云主机
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



    public static void main(String[] args) throws IOException, SQLException {//主方法
        MySQLLoaderOptimize h = new MySQLLoaderOptimize();//创建本类对象
        con = h.getConnection(2);//与数据库建立连接

//        pStmt = con.prepareStatement("select count(*) from movie_review");
//        res = pStmt.executeQuery();
//        res.next();
//        System.out.println(res.getInt(1));

//        addToMovieReviewByBatch();
//        addToMovieReviewByBatch();
//        initiate_date();
        addIndex();
    }

    //添加索引
    public static void addIndex() throws SQLException {
        pStmt = con.prepareStatement("alter table movie_review add index (score)");
        pStmt.execute();
    }

    //将movieInfo一个一个地添加到数据库
    public static void addToMovieInfoOneByOne() throws IOException {
        //将json文件解析成Movie对象
        BufferedReader reader = new BufferedReader(new FileReader("D:\\DataWarehouse\\code\\DW\\Data\\part-r-00000(1)"));
        String line;
        line = reader.readLine();
        int index = 1;
        while(line!=null)
//        while(index < 4000)
        {
            //读取一个json对象

                System.out.println("Reading Object " + index);
            if (index >= 187682) {
                Movie movie = JSON.parseObject(line, Movie.class);


                //初始化待操作变量
                List<String> amazon_movie_ids = movie.getId();
                int movie_id = index;
                String title = movie.getTitle();
                List<String> directors = movie.getDirector();
                Date releaseDate = movie.getReleaseDate();
                List<String> types = movie.getType();
                List<String> versions = movie.getVersion();
                List<String> actors = movie.getActor();

                //添加至movie_info_fact
                try {
                    pStmt = con.prepareStatement("insert into movie_info_fact values(?,?,?,?,?)");
                    pStmt.setInt(1, movie_id);
                    //添加movie_title
                    if(title != null)
                        pStmt.setString(2, delete_Quotation_Mark(title));
                    else
                        pStmt.setNull(2, Types.VARCHAR);
                    //添加year, weekday和date_key
                    if(releaseDate == null){
                        pStmt.setNull(3,Types.INTEGER);
                        pStmt.setNull(4,Types.INTEGER);
                        pStmt.setNull(5,Types.INTEGER);
                    }
                    else{
                        pStmt.setInt(3,releaseDate.getYear() + 1900);
                        pStmt.setInt(4,releaseDate.getDay());
                        //计算date_key
                        PreparedStatement pStmt2 = con.prepareStatement(
                                "select date_key from movie_release_date where month=? and date=?");
                        pStmt2.setInt(1,releaseDate.getMonth()+1);
                        pStmt2.setInt(2,releaseDate.getDate());
                        res = pStmt2.executeQuery();
                        res.next();
                        pStmt.setInt(5,res.getInt("date_key"));
                        //释放资源
                        res.close();
                        pStmt2.close();
                    }
                    pStmt.executeUpdate();
                } catch (SQLException e) {
                    System.out.println("There's an exception while inserting into movie_info_fact");
                    e.printStackTrace();
                }

                //添加至movie_amazon_id
                try {
                    pStmt = con.prepareStatement("insert into movie_amazon_id values(?, ?)");
                    pStmt.setInt(1, movie_id);
                } catch (SQLException e) {
                    System.out.println("There's an exception while inserting into movie_amazon_id");
                    e.printStackTrace();
                }
                for(String temp: amazon_movie_ids){
                    try {
                        pStmt.setString(2, temp);
                        pStmt.executeUpdate();
                    } catch (SQLException e) {
                        System.out.println("There's an exception while inserting into movie_amazon_id");
                        e.printStackTrace();
                    }
                }

                //添加至movie_director
                try {
                    pStmt = con.prepareStatement("insert into movie_director values(?, ?)");
                    pStmt.setInt(1, movie_id);
                } catch (SQLException e) {
                    System.out.println("There's an exception while inserting into movie_director");
                    e.printStackTrace();
                }
                for(String temp: directors) {
                    try {
                        pStmt.setString(2, temp);
                        pStmt.executeUpdate();
                    } catch (SQLException e) {
                        System.out.println("There's an exception while inserting into movie_director");
                        e.printStackTrace();
                    }
                }

                //添加至movie_type
                try {
                    pStmt = con.prepareStatement("insert into movie_type values(?, ?)");
                    pStmt.setInt(1, movie_id);
                } catch (SQLException e) {
                    System.out.println("There's an exception while inserting into movie_type");
                    e.printStackTrace();
                }
                for(String temp: types) {
                    try {
                        pStmt.setString(2, temp);
                        pStmt.executeUpdate();
                    } catch (SQLException e) {
                        System.out.println("There's an exception while inserting into movie_type");
                        e.printStackTrace();
                    }
                }

                //添加至movie_version
                try {
                    pStmt = con.prepareStatement("insert into movie_version values(?, ?)");
                    pStmt.setInt(1, movie_id);
                } catch (SQLException e) {
                    System.out.println("There's an exception while inserting into movie_version");
                    e.printStackTrace();
                }
                for(String temp: versions) {
                    try {
                        pStmt.setString(2, temp);
                        pStmt.executeUpdate();
                    } catch (SQLException e) {
                        System.out.println("There's an exception while inserting into movie_version");
                        e.printStackTrace();
                    }
                }

                //添加至movie_actor
                try {
                    pStmt = con.prepareStatement("insert into movie_actor values(?, ?, ?)");
                    pStmt.setInt(1, movie_id);
                } catch (SQLException e) {
                    System.out.println("There's an exception while inserting into movie_actor");
                    e.printStackTrace();
                }
                boolean is_lead = true;
                for(String temp: actors) {
                    try {
                        pStmt.setString(2, temp);
                        if(is_lead) {
                            pStmt.setBoolean(3, true);
                            is_lead = false;
                        }
                        else
                            pStmt.setBoolean(3, false);
                        pStmt.executeUpdate();
                    } catch (SQLException e) {
                        System.out.println("There's an exception while inserting into movie_actor");
                        e.printStackTrace();
                    }
                }
            }

            line = reader.readLine();
            index++;
        }
    }

    //将movieInfo一批一批地添加到数据库（此方法有bug）
    public static void addToMovieInfoByBatch() throws IOException, SQLException {
        //将json文件解析成Movie对象
        BufferedReader reader = new BufferedReader(new FileReader("D:\\DataWarehouse\\code\\DW\\Data\\part-r-00000(1)"));
        String line;
        line = reader.readLine();

        con.setAutoCommit(false);


        PreparedStatement pStmt_fact = con.prepareStatement("insert into movie_info_fact values(?,?,?,?,?)");
        PreparedStatement pStmt_amazonID = con.prepareStatement("insert into movie_amazon_id values(?, ?)");
        PreparedStatement pStmt_director = con.prepareStatement("insert into movie_director values(?, ?)");
        PreparedStatement pStmt_type = con.prepareStatement("insert into movie_type values(?, ?)");
        PreparedStatement pStmt_version = con.prepareStatement("insert into movie_version values(?, ?)");
        PreparedStatement pStmt_actor = con.prepareStatement("insert into movie_actor values(?, ?, ?)");

        int index = 1;
//        while(line!=null)
        while(index <= 3000)
        {
            try {
                //读取一个json对象
                System.out.println("Reading Object " + index);
                Movie movie = JSON.parseObject(line, Movie.class);

                //初始化待操作变量
                List<String> amazon_movie_ids = movie.getId();
                int movie_id = index;
                String title = movie.getTitle();
                List<String> directors = movie.getDirector();
                java.util.Date releaseDate = movie.getReleaseDate();
                List<String> types = movie.getType();
                List<String> versions = movie.getVersion();
                List<String> actors = movie.getActor();

                //添加至movie_info_fact
                pStmt_fact.setInt(1, movie_id);
                //添加movie_title
                if(title != null)
                    pStmt_fact.setString(2, delete_Quotation_Mark(title));
                else
                    pStmt_fact.setNull(2, Types.VARCHAR);
                //添加year, weekday和date_key
                if(releaseDate == null){
                    pStmt_fact.setNull(3,Types.INTEGER);
                    pStmt_fact.setNull(4,Types.INTEGER);
                    pStmt_fact.setNull(5,Types.INTEGER);
                }
                else{
                    pStmt_fact.setInt(3,releaseDate.getYear() + 1900);
                    pStmt_fact.setInt(4,releaseDate.getDay());
                    //计算date_key
                    PreparedStatement pStmt2 = con.prepareStatement(
                            "select date_key from movie_release_date where month=? and date=?");
                    pStmt2.setInt(1,releaseDate.getMonth()+1);
                    pStmt2.setInt(2,releaseDate.getDate());
                    res = pStmt2.executeQuery();
                    res.next();
                    pStmt_fact.setInt(5,res.getInt("date_key"));
                    //释放资源
                    res.close();
                    pStmt2.close();
                }
                pStmt_fact.addBatch();

                //添加至movie_amazon_id
                pStmt_amazonID.setInt(1, movie_id);
                for(String temp: amazon_movie_ids){
                    pStmt_amazonID.setString(2, temp);
                    pStmt_amazonID.addBatch();
                }

                //添加至movie_director
                pStmt_director.setInt(1, movie_id);
                for(String temp: directors) {
                    pStmt_director.setString(2, temp);
                    pStmt_director.addBatch();
                }

                //添加至movie_release_date
//                if(releaseDate != null){
//                    pStmt = con.prepareStatement("insert into movie_release_date values(?, ?, ?, ?, ?, ?)");
//                    pStmt.setString(1, movie_id);
//                    //年份
//                    pStmt.setInt(2, releaseDate.getYear() + 1900);
//                    //月份
//                    int month = releaseDate.getMonth() + 1;
//                    pStmt.setInt(3, month);
//                    //季度和上下半年
//                    int year_quarter;
//                    int year_half;
//                    if(month <= 3) {
//                        year_quarter = 1;
//                        year_half = 1;
//                    }
//                    else if(month <= 6) {
//                        year_quarter = 2;
//                        year_half = 1;
//                    }
//                    else if(month <= 9) {
//                        year_quarter = 3;
//                        year_half = 2;
//                    }
//                    else {
//                        year_quarter = 4;
//                        year_half = 2;
//                    }
//                    pStmt.setInt(4, year_quarter);
//                    pStmt.setInt(5, year_half);
//                    //星期几
//                    pStmt.setInt(6, releaseDate.getDay());
//                    //几号
//                    System.out.println(releaseDate.getDate());
//
//                    pStmt.executeUpdate();
//                }

                //添加至movie_type
                pStmt_type.setInt(1, movie_id);
                for(String temp: types) {
                    pStmt_type.setString(2, temp);
                    pStmt_type.addBatch();
                }

                //添加至movie_version
                pStmt_version.setInt(1, movie_id);
                for(String temp: versions) {
                    pStmt_version.setString(2, temp);
                    pStmt_version.addBatch();
                }

                //添加至movie_actor
                pStmt_actor.setInt(1, movie_id);
                boolean is_lead = true;
                for(String temp: actors) {
                    pStmt_actor.setString(2, temp);
                    if(is_lead) {
                        pStmt_actor.setBoolean(3, true);
                        is_lead = false;
                    }
                    else
                        pStmt_actor.setBoolean(3, false);
                    pStmt_actor.addBatch();
                }

                //每读一千个json提交一次
                if(index % 1000 == 0){
                    pStmt_fact.executeBatch();
                    pStmt_amazonID.executeBatch();
                    pStmt_director.executeBatch();
                    pStmt_type.executeBatch();
                    pStmt_version.executeBatch();
                    pStmt_actor.executeBatch();
                    con.commit();
                    pStmt_fact.clearBatch();
                    pStmt_amazonID.clearBatch();
                    pStmt_director.clearBatch();
                    pStmt_type.clearBatch();
                    pStmt_version.clearBatch();
                    pStmt_actor.clearBatch();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            line = reader.readLine();
            index++;
        }
        try {
            pStmt_fact.executeBatch();
            pStmt_amazonID.executeBatch();
            pStmt_director.executeBatch();
            pStmt_type.executeBatch();
            pStmt_version.executeBatch();
            pStmt_actor.executeBatch();
            con.commit();
            pStmt_fact.clearBatch();
            pStmt_amazonID.clearBatch();
            pStmt_director.clearBatch();
            pStmt_type.clearBatch();
            pStmt_version.clearBatch();
            pStmt_actor.clearBatch();
            con.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    //初始化movie_release_date表
    public static void initiate_date() throws SQLException {
        pStmt = con.prepareStatement("insert into movie_release_date values(?,?,?,?,?)");
        //一月
        for(int i = 1; i <= 31; i++){
            pStmt.setInt(1,i);//date_key
            pStmt.setInt(2,i);//date
            pStmt.setInt(3,1);//month
            pStmt.setInt(4,1);//year_quarter
            pStmt.setInt(5,1);//year_half
            pStmt.executeUpdate();
        }
        //二月
        for(int i = 1; i <= 29; i++){
            pStmt.setInt(1,31 + i);//date_key
            pStmt.setInt(2,i);//date
            pStmt.setInt(3,2);//month
            pStmt.setInt(4,1);//year_quarter
            pStmt.setInt(5,1);//year_half
            pStmt.executeUpdate();
        }
        //三月
        for(int i = 1; i <= 31; i++){
            pStmt.setInt(1,60 + i);//date_key
            pStmt.setInt(2,i);//date
            pStmt.setInt(3,3);//month
            pStmt.setInt(4,1);//year_quarter
            pStmt.setInt(5,1);//year_half
            pStmt.executeUpdate();
        }
        //四月
        for(int i = 1; i <= 30; i++){
            pStmt.setInt(1,91 + i);//date_key
            pStmt.setInt(2,i);//date
            pStmt.setInt(3,4);//month
            pStmt.setInt(4,2);//year_quarter
            pStmt.setInt(5,1);//year_half
            pStmt.executeUpdate();
        }
        //五月
        for(int i = 1; i <= 31; i++){
            pStmt.setInt(1,121 + i);//date_key
            pStmt.setInt(2,i);//date
            pStmt.setInt(3,5);//month
            pStmt.setInt(4,2);//year_quarter
            pStmt.setInt(5,1);//year_half
            pStmt.executeUpdate();
        }
        //六月
        for(int i = 1; i <= 30; i++){
            pStmt.setInt(1,152 + i);//date_key
            pStmt.setInt(2,i);//date
            pStmt.setInt(3,6);//month
            pStmt.setInt(4,2);//year_quarter
            pStmt.setInt(5,1);//year_half
            pStmt.executeUpdate();
        }
        //七月
        for(int i = 1; i <= 31; i++){
            pStmt.setInt(1,182 + i);//date_key
            pStmt.setInt(2,i);//date
            pStmt.setInt(3,7);//month
            pStmt.setInt(4,3);//year_quarter
            pStmt.setInt(5,2);//year_half
            pStmt.executeUpdate();
        }
        //八月
        for(int i = 1; i <= 31; i++){
            pStmt.setInt(1,213 + i);//date_key
            pStmt.setInt(2,i);//date
            pStmt.setInt(3,8);//month
            pStmt.setInt(4,3);//year_quarter
            pStmt.setInt(5,2);//year_half
            pStmt.executeUpdate();
        }
        //九月
        for(int i = 1; i <= 30; i++){
            pStmt.setInt(1,244 + i);//date_key
            pStmt.setInt(2,i);//date
            pStmt.setInt(3,9);//month
            pStmt.setInt(4,3);//year_quarter
            pStmt.setInt(5,2);//year_half
            pStmt.executeUpdate();
        }
        //十月
        for(int i = 1; i <= 31; i++){
            pStmt.setInt(1,274 + i);//date_key
            pStmt.setInt(2,i);//date
            pStmt.setInt(3,10);//month
            pStmt.setInt(4,4);//year_quarter
            pStmt.setInt(5,2);//year_half
            pStmt.executeUpdate();
        }
        //十一月
        for(int i = 1; i <= 30; i++){
            pStmt.setInt(1,305 + i);//date_key
            pStmt.setInt(2,i);//date
            pStmt.setInt(3,11);//month
            pStmt.setInt(4,4);//year_quarter
            pStmt.setInt(5,2);//year_half
            pStmt.executeUpdate();
        }
        //十二月
        for(int i = 1; i <= 31; i++){
            pStmt.setInt(1,335 + i);//date_key
            pStmt.setInt(2,i);//date
            pStmt.setInt(3,12);//month
            pStmt.setInt(4,4);//year_quarter
            pStmt.setInt(5,2);//year_half
            pStmt.executeUpdate();
        }
    }

    //将review一批一批地添加到数据库
    public static void addToMovieReviewByBatch() throws IOException, SQLException {
        BufferedReader reader = new BufferedReader(new FileReader("D:\\movies.txt\\movies.txt"));

        con.setAutoCommit(false);
        pStmt = con.prepareStatement("insert into movie_review values (?,?,?,?,?,?,?,?,?)");

        String line = reader.readLine();
        int i = 1, wrong_num_1 = 0, wrong_num_2 = 0;
        long startTime =  System.currentTimeMillis();//开始计时
        while(line!=null)
//        while(i <= 1000)
        {
            System.out.println("Review "+i);
            if (i >= 7351501) {//有机会从三百万再插一遍
                try {
                    //【注】一个影评占八行 + 一个空行
                    // 第一行product_id
                    String product_id_origin = line.substring(19);
                    PreparedStatement pStmt2 = con.prepareStatement(
                            "select movie_id from movie_amazon_id where amazon_id=?");
                    pStmt2.setString(1,product_id_origin);
                    ResultSet resultSet = pStmt2.executeQuery();
                    if(resultSet.next())
                        pStmt.setInt(1,resultSet.getInt(1));
                    else {
                        System.out.println("The movie to which this review refers doesn't exist");
                        pStmt.setInt(1,-1);
    //                    i++;
    //                    continue;
                    }
    //                pStmt.setString(1, product_id_origin);

                    //第二行userID
                    line = reader.readLine();
                    String userID = line.substring(15);
                    pStmt.setString(2,userID);

                    //第三行profileName
                    line = reader.readLine();
                    String profileName = line.substring(20);
                    pStmt.setString(3,profileName);

                    //第四行helpfulness
                    line = reader.readLine();
                    String helpfulness = line.substring(20);
                    String[] spilt = helpfulness.split("/");
                    int num_reader = Integer.parseInt(spilt[1]);
                    int num_helpfulness = Integer.parseInt(spilt[0]);
                    pStmt.setInt(4,num_reader);
                    pStmt.setInt(5,num_helpfulness);

                    //第五行score
                    line = reader.readLine();
                    String stringScore = line.substring(14);
                    float score = Float.parseFloat(stringScore);
                    pStmt.setFloat(6,score);

                    //第六行time
                    line = reader.readLine();
                    String stringTime = line.substring(13);
                    Long longTime = Long.parseLong(stringTime);
                    java.sql.Date time = new java.sql.Date(longTime);
                    pStmt.setDate(7,time);

                    //第七行summary
                    line = reader.readLine();
                    String summary = line.substring(16);
                    pStmt.setString(8,summary);

                    //第八行text
                    line = reader.readLine();
                    String text = line.substring(13);
                    pStmt.setString(9,text);

                    //第九行空行
                    line = reader.readLine();

                    //执行insert
                    pStmt.addBatch();
                }catch (Exception e){
                    wrong_num_1++;
                    e.printStackTrace();
                }

                //每100个提交一次
                if(i % 100 == 0){
                    System.out.println("--------------submit--------------");
                    try {
                        pStmt.executeBatch();
                        con.commit();
                        pStmt.clearBatch();
                    } catch (SQLException e) {
                        wrong_num_2++;
                        e.printStackTrace();
                    }
                }
            }
            else{
                for(int j = 0; j < 8; j++)
                    line = reader.readLine();
            }
            i++;
            line = reader.readLine();
        }
        try {
            //处理剩下一小部分
            pStmt.executeBatch();
            con.commit();
            pStmt.clearBatch();
            //con.close();
        } catch (SQLException e) {
            wrong_num_2++;
            e.printStackTrace();
        }

        long endTime =  System.currentTimeMillis();
        long usedTime = (endTime-startTime)/1000;
        System.out.println("一共用时"+usedTime+"秒");
        System.out.println("出错个数为"+wrong_num_1+" | "+wrong_num_2);
    }

    //去除字符串外层引号
    @org.jetbrains.annotations.NotNull
    public static String delete_Quotation_Mark(String s_origin) throws SQLException {
        if(s_origin.charAt(0) == '\"' && s_origin.charAt(s_origin.length() - 1) == '\"'){
            return s_origin.substring(1, s_origin.length() - 1);
        }
        else
            return s_origin;
    }
}
