package com.f4.DWQueryServer;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @program: DW
 * @description:
 * @author: Zijian Zhang
 * @create: 2019/11/28
 **/
@SpringBootApplication
public class QueryServerMain {
    @Value("${neo4j.uri}")
    String neo4jURL;
    @Value("${neo4j.username}")
    String neo4jUserName;
    @Value("${neo4j.password}")
    String neo4jPassword;
    public static void main(String[] args) {
        SpringApplication.run(QueryServerMain.class, args);
    }
    @Bean
    Driver getNeo4jDriver(){
        try {
            return GraphDatabase.driver( neo4jURL, AuthTokens.basic(neo4jUserName,neo4jPassword));
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }
    @Bean
    Connection getConnection() {
        int host = 2;

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
                System.out.println("成功连接localhost:3306/mysql数据库");
                return DriverManager.getConnection(url, user, password);
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }
        else if(host == 2){
            try {
                // 协议：子协议：//目标IP地址：端口/数据库  在这里test1是之前创建的数据库名
                String url = "jdbc:mysql://am-bp1fhqofzb198v9f990650o.ads.aliyuncs.com:3306/datawarehouse";
                String user = "dw";
                String password = "Zzj6p@saturn@lym@tyl";
                System.out.println("成功连接am-bp1fhqofzb198v9f990650o.ads.aliyuncs.com:3306/datawarehouse数据库");
                return DriverManager.getConnection(url, user, password);
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }
        else
            return null;
    }
}
