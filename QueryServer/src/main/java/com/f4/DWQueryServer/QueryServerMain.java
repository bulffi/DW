package com.f4.DWQueryServer;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

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
}
