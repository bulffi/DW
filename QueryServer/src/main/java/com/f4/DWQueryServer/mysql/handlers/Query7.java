package com.f4.DWQueryServer.mysql.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import com.f4.DWQueryServer.mysql.MySQLMainHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * XXXXXXXXX科技有限公司 版权所有 © Copyright 2018<br>
 *
 * @Description: <br>
 * @Project: hades <br>
 * @CreateDate: Created in 2019/12/11 16:19 <br>
 * @Author: <a href="abc@qq.com">abc</a>
 */
@Service
public class Query7 { //按用户评价进行查询，默认为评分
    @Autowired
    Connection connection;

    public DataAnswer getDataAnswer(SpecificQuery query) throws SQLException {
        //要用的数据
        String userID = query.getComment().getUser_id();
        double score_from = query.getComment().getScore_from();
        double score_to = query.getComment().getScore_to();
        String answer = query.getAnswer();

        //初始化返回值
        DataAnswer dataAnswer = new DataAnswer();
        List<String> data = new ArrayList<>();

        if(!userID.isEmpty()){//某用户喜欢哪些电影
            PreparedStatement preparedStatement;
            if(!MySQLMainHandler.optimize){//不优化
                preparedStatement = connection.prepareStatement(
                        "select movie_title " +
                                "from movie_info_fact " +
                                "where movie_id in " +
                                "(" +
                                "select productID " +
                                "from movie_review " +
                                "where userID = ? and score >= 4" +
                                ")");
            }
            else {//优化
                preparedStatement = connection.prepareStatement(
                        "select movie_title " +
                                "from prejoin_movie_review " +
                                "where userID = ? and score >= 4");
            }
            preparedStatement.setString(1, userID);

            //执行sql语句并计时
            long startTime =  System.currentTimeMillis();//开始计时
            ResultSet resultSet = preparedStatement.executeQuery();//执行sql
            long endTime =  System.currentTimeMillis();//结束计时
            long usedTime = endTime-startTime;//计算耗时(单位为毫秒)

            while (resultSet.next()){
                data.add(resultSet.getString(1));
            }
            dataAnswer.setData(data);
            dataAnswer.setTime(usedTime);
            return dataAnswer;
        }
        if(score_from > score_to){ //分数段无效
            data.add("请输入合法的分数段");
            dataAnswer.setData(data);
            dataAnswer.setTime(Long.valueOf(0));
            return dataAnswer;
        }

        long usedTime;
        if(answer.equals("comment")){//查询某分数段的评论
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "select * from movie_review " +
                            "where score >= ? and score <= ?");
            preparedStatement.setDouble(1, score_from);
            preparedStatement.setDouble(2, score_to);

            //执行sql语句并计时
            long startTime =  System.currentTimeMillis();//开始计时
            ResultSet resultSet = preparedStatement.executeQuery();//执行sql
            long endTime =  System.currentTimeMillis();//结束计时
            usedTime = endTime-startTime;//计算耗时(单位为毫秒)

            int i = 0;
            while(resultSet.next()){
                i ++;
                String profileName = resultSet.getString("profileName");
                String score = String.valueOf(resultSet.getDouble("score"));
                Date date = resultSet.getDate("time");
                String year = String.valueOf(date.getYear() + 1900);
                String month = String.valueOf(date.getMonth() + 1);
                String day = String.valueOf(date.getDate());
                String summary = resultSet.getString("summary");
                data.add(String.valueOf(profileName + " -- " + year + "/" + month + "/" + day + " -- " + score + " -- " + summary));
                if(i >= 1000){//最多发一千条
                    break;
                }
            }
        }
        else{//查询平均评分在某分数段的电影有哪些
            PreparedStatement preparedStatement;
            if(!MySQLMainHandler.optimize){//不优化
                preparedStatement = connection.prepareStatement(
                        "select movie_title " +
                                "from movie_info_fact " +
                                "where movie_id in " +
                                "(" +
                                "select productID " +
                                "from movie_review " +
                                "group by productID " +
                                "having avg(score) >= ? and avg(score) <= ?" +
                                ")");
            }
            else {//优化
                preparedStatement = connection.prepareStatement(
                        "select movie_title " +
                                "from precal_movie_avg_score " +
                                "where avg_score >= ? and avg_score <= ?");
            }
            preparedStatement.setDouble(1, score_from);
            preparedStatement.setDouble(2, score_to);

            //执行sql语句并计时
            long startTime =  System.currentTimeMillis();//开始计时
            ResultSet resultSet = preparedStatement.executeQuery();//执行sql
            long endTime =  System.currentTimeMillis();//结束计时
            usedTime = endTime-startTime;//计算耗时(单位为毫秒)

            while(resultSet.next()){
                data.add(resultSet.getString(1));
            }
        }

        dataAnswer.setData(data);
        dataAnswer.setTime(usedTime);
        return dataAnswer;
    }

    //只返回时间
    public TestAnswer getTestAnswer(SpecificQuery query) throws SQLException {
        TestAnswer testAnswer = new TestAnswer();
        testAnswer.setTime(getDataAnswer(query).getTime());
        return testAnswer;
    }
}
