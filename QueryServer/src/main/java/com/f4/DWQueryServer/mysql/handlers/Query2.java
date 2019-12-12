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
import java.util.List;

/**
 * XXXXXXXXX科技有限公司 版权所有 © Copyright 2018<br>
 *
 * @Description: <br>
 * @Project: hades <br>
 * @CreateDate: Created in 2019/12/11 16:18 <br>
 * @Author: <a href="abc@qq.com">abc</a>
 */
@Service
public class Query2 { //按电影名称查询
    @Autowired
    Connection connection;

    public DataAnswer getDataAnswer(SpecificQuery query) throws SQLException {
        String title = query.getMovie_name();
        String answer = query.getAnswer();
        PreparedStatement preparedStatement;//声明查询语句

        if(answer.equals("date")){ //* 电影上映日期是多少
            preparedStatement = connection.prepareStatement(
                    "select year, month, date " +
                            "from movie_info_fact natural join movie_release_date " +
                            "where movie_title = ?");
            preparedStatement.setString(1, title);

            //执行sql语句并计时
            long startTime =  System.currentTimeMillis();//开始计时
            ResultSet resultSet = preparedStatement.executeQuery();//执行sql
            long endTime =  System.currentTimeMillis();//结束计时
            long usedTime = endTime-startTime;//计算耗时(单位为毫秒)

            resultSet.next();
            String date = resultSet.getString(1) + '-'
                    + resultSet.getString(2) + '-'
                    + resultSet.getString(3);
            List<String> list = new ArrayList<String>();
            list.add(date);

            DataAnswer dataAnswer = new DataAnswer();
            dataAnswer.setData(list);
            dataAnswer.setTime(usedTime);
            return dataAnswer;
        }
        else {
            if(answer.equals("count")){ // * 电影有多少同名电影
                preparedStatement = connection.prepareStatement(
                        "select count(*) from movie_info_fact where movie_title = ?");
            }
            else if(answer.equals("actor")){ // * 电影有哪些演员
                preparedStatement = connection.prepareStatement(
                        "select actor " +
                                "from movie_info_fact natural join movie_actor " +
                                "where movie_title = ?");
            }
            else if (answer.equals("director")){ // * 电影有哪些导演
                preparedStatement = connection.prepareStatement(
                        "select director_name " +
                                "from movie_info_fact natural join movie_director " +
                                "where movie_title = ?");
            }
            else if (answer.equals("type")){ // * 电影是什么类型的
                preparedStatement = connection.prepareStatement(
                        "select type " +
                                "from movie_info_fact natural join movie_type " +
                                "where movie_title = ?");
            }
            else if (answer.equals("version")){ // * 电影是有哪些版本
                preparedStatement = connection.prepareStatement(
                        "select version " +
                                "from movie_info_fact natural join movie_version " +
                                "where movie_title = ?");
            }
            else {// default情况
                DataAnswer dataAnswer = new DataAnswer();
                dataAnswer.setTime((long) 0);
                dataAnswer.setData(new ArrayList<>());
                return dataAnswer;
            }
            preparedStatement.setString(1, title);
        }

        //执行sql语句并计时
        long startTime =  System.currentTimeMillis();//开始计时
        ResultSet resultSet = preparedStatement.executeQuery();//执行sql
        long endTime =  System.currentTimeMillis();//结束计时
        long usedTime = endTime-startTime;//计算耗时(单位为毫秒)

        DataAnswer dataAnswer = new DataAnswer();
        dataAnswer.setTime(usedTime);
        List<String> data = new ArrayList<String>();
        while (resultSet.next()){
            if(answer.equals("count"))
                data.add(String.valueOf(resultSet.getInt(1)));
            else
                data.add(resultSet.getString(1));
        }
        dataAnswer.setData(data);

        return dataAnswer;
    }

    //只返回时间
    public TestAnswer getTestAnswer(SpecificQuery query) throws SQLException {
        TestAnswer testAnswer = new TestAnswer();
        testAnswer.setTime(getDataAnswer(query).getTime());
        return testAnswer;
    }
}
