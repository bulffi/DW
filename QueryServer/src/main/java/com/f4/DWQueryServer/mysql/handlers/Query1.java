package com.f4.DWQueryServer.mysql.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * XXXXXXXXX科技有限公司 版权所有 © Copyright 2018<br>
 *
 * @Description: <br>
 * @Project: hades <br>
 * @CreateDate: Created in 2019/12/11 16:17 <br>
 * @Author: <a href="abc@qq.com">abc</a>
 */
@Service
public class Query1 {//按时间查询
    @Autowired
    Connection connection;

    //既返回结果，也返回时间
    public DataAnswer getDataAnswer(SpecificQuery query) throws SQLException {
        SpecificQuery.DWTime timeFrom = query.getTime_from(); //起始时间表
        SpecificQuery.DWTime timeTo = query.getTime_to(); //结束时间表

        int year = timeFrom.getYear();//年份
        int month = timeFrom.getMonth();//月份
        int quarter = timeFrom.getQuarter();//季度
        int day = timeFrom.getDay_of_week();//周几

        String answer = query.getAnswer();//返回给前端的结果类型

        PreparedStatement preparedStatement;//声明查询语句

        //初始化查询语句
        if (timeFrom.getYear() == 0) { //按星期查询
            if(answer.equals("count")) {//星期 * 上映电影的数量
                preparedStatement = connection.prepareStatement(
                        "select count(*) from movie_info_fact where weekday = ?");
            }
            else if(answer.equals("title")){ //星期 * 上映电影的名字
                preparedStatement = connection.prepareStatement(
                        "select movie_title from movie_info_fact where weekday = ?");
            }
            else if(answer.equals("id")){ //星期 * 上映电影的id
                preparedStatement = connection.prepareStatement(
                        "select movie_id from movie_info_fact where weekday = ?");
            }
            else { //default情况
                DataAnswer dataAnswer = new DataAnswer();
                dataAnswer.setTime((long) 0);
                dataAnswer.setData(new ArrayList<>());
                return dataAnswer;
            }
            preparedStatement.setInt(1, day);
        }
        else { //按年份查询
            if(month == 0 && quarter == 0) {//只按年份查询
                if(answer.equals("count")) {// * 年上映电影的数量
                    preparedStatement = connection.prepareStatement(
                            "select count(*) from movie_info_fact where year = ?");
                }
                else if(answer.equals("title")){ // * 年上映电影的名字
                    preparedStatement = connection.prepareStatement(
                            "select movie_title from movie_info_fact where year = ?");
                }
                else if(answer.equals("id")){ // * 年上映电影的id
                    preparedStatement = connection.prepareStatement(
                            "select movie_id from movie_info_fact where year = ?");
                }
                else { //default情况
                    DataAnswer dataAnswer = new DataAnswer();
                    dataAnswer.setTime((long) 0);
                    dataAnswer.setData(new ArrayList<>());
                    return dataAnswer;
                }
                preparedStatement.setInt(1, year);
            }
            else { //不只按年份查找
                if(month != 0) { // 按 * 年 * 月查询
                    if(answer.equals("count")) { //* 年 * 月上映电影的数量
                        preparedStatement = connection.prepareStatement(
                                "select count(*) " +
                                        "from movie_info_fact " +
                                        "natural join movie_release_date " +
                                        "where year = ? and month = ?");
                    }
                    else if(answer.equals("title")) { // * 年 * 月上映电影的名字
                        preparedStatement = connection.prepareStatement(
                                "select movie_title " +
                                        "from movie_info_fact " +
                                        "natural join movie_release_date " +
                                        "where year = ? and month = ?");
                    }
                    else if (answer.equals("id")) { // * 年 * 月上映电影的id
                        preparedStatement = connection.prepareStatement(
                                "select movie_id " +
                                        "from movie_info_fact " +
                                        "natural join movie_release_date " +
                                        "where year = ? and month = ?");
                    }
                    else { //default情况
                        DataAnswer dataAnswer = new DataAnswer();
                        dataAnswer.setTime((long) 0);
                        dataAnswer.setData(new ArrayList<>());
                        return dataAnswer;
                    }
                    preparedStatement.setInt(1, year);
                    preparedStatement.setInt(2, month);
                }
                else { //按 * 年 * 季度查询
                    if(answer.equals("count")) { //* 年 * 季度上映电影的数量
                        preparedStatement = connection.prepareStatement(
                                "select count(*) " +
                                        "from movie_info_fact " +
                                        "natural join movie_release_date " +
                                        "where year = ? and year_quarter = ?");
                    }
                    else if(answer.equals("title")) { // * 年 * 季度上映电影的名字
                        preparedStatement = connection.prepareStatement(
                                "select movie_title " +
                                        "from movie_info_fact " +
                                        "natural join movie_release_date " +
                                        "where year = ? and year_quarter = ?");
                    }
                    else if (answer.equals("id")) { // * 年 * 季度上映电影的id
                        preparedStatement = connection.prepareStatement(
                                "select movie_id " +
                                        "from movie_info_fact " +
                                        "natural join movie_release_date " +
                                        "where year = ? and year_quarter = ?");
                    }
                    else { //default情况
                        DataAnswer dataAnswer = new DataAnswer();
                        dataAnswer.setTime((long) 0);
                        dataAnswer.setData(new ArrayList<>());
                        return dataAnswer;
                    }
                    preparedStatement.setInt(1, year);
                    preparedStatement.setInt(2, quarter);
                }
            }
        }

        //执行sql语句并计时
        long startTime =  System.currentTimeMillis();//开始计时
        ResultSet resultSet = preparedStatement.executeQuery();//执行sql
        long endTime =  System.currentTimeMillis();//结束计时
        //double usedTime = (endTime-startTime)/1000.0;//计算耗时(单位为秒)
        long usedTime = endTime-startTime;

        DataAnswer dataAnswer = new DataAnswer();
        List<String> data = new ArrayList<String>();
        while (resultSet.next()){
            if(answer.equals("title") || answer.equals("id"))
                data.add(resultSet.getString(1));
            else
                data.add(String.valueOf(resultSet.getInt(1)));
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
