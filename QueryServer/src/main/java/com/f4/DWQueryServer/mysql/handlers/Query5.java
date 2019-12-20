package com.f4.DWQueryServer.mysql.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.CollaborateQuery;
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
 * @CreateDate: Created in 2019/12/11 16:18 <br>
 * @Author: <a href="abc@qq.com">abc</a>
 */

@Service
public class Query5 { //按合作关系查询
    @Autowired
    Connection connection;

    public DataAnswer getDataAnswer(CollaborateQuery query) throws SQLException {
        int type = query.getType();
        int threshold = query.getThreshold();

        DataAnswer dataAnswer = new DataAnswer();
        if (type < 1 && type > 3) {
            return dataAnswer;
        }

        PreparedStatement preparedStatement;//声明查询语句
        if (type == 1) {//哪些演员经常合作
            preparedStatement = connection.prepareStatement(
                    "select * from precal_collaboration_actor " +
                            "where collaboration_time >= ?");

        }
        else if (type == 2) {//哪些演员和导演经常合作
            preparedStatement = connection.prepareStatement(
                    "select * from precal_collaboration_director_actor " +
                            "where collaboration_time >= ?");
        }
        else {//哪些导演经常合作
            preparedStatement = connection.prepareStatement(
                    "select * from precal_collaboration_director " +
                            "where collaboration_time >= ?");
        }
        preparedStatement.setInt(1, threshold);

        //执行sql语句并计时
        long startTime =  System.currentTimeMillis();//开始计时
        ResultSet resultSet = preparedStatement.executeQuery();//执行sql
        long endTime =  System.currentTimeMillis();//结束计时
        long usedTime = endTime-startTime;//计算耗时(单位为毫秒)


        dataAnswer.setTime(usedTime);
        List<String> data = new ArrayList<String>();
        while (resultSet.next()){
            data.add(resultSet.getString(1) + " & " + resultSet.getString(2) +
                    " for " + resultSet.getInt(3) + "time(s)");
        }
        dataAnswer.setData(data);

        return dataAnswer;
    }

    //只返回时间
    public TestAnswer getTestAnswer(CollaborateQuery query) throws SQLException {
        TestAnswer testAnswer = new TestAnswer();
        testAnswer.setTime(getDataAnswer(query).getTime());
        return testAnswer;
    }
}
