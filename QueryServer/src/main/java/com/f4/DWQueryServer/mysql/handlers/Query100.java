package com.f4.DWQueryServer.mysql.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
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
 * @CreateDate: Created in 2019/12/22 20:39 <br>
 * @Author: <a href="abc@qq.com">abc</a>
 */
@Service
public class Query100 {
    @Autowired
    Connection connection;

    public DataAnswer getDataAnswer(String sql) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //执行sql语句并计时
        long startTime =  System.currentTimeMillis();//开始计时
        ResultSet resultSet = preparedStatement.executeQuery();//执行sql
        long endTime =  System.currentTimeMillis();//结束计时
        long usedTime = endTime-startTime;//计算耗时(单位为毫秒)

        //初始化返回值
        DataAnswer dataAnswer = new DataAnswer();
        List<String> data = new ArrayList<>();
        while(resultSet.next()){
            data.add(resultSet.getString(1));
        }
        dataAnswer.setData(data);
        dataAnswer.setTime(usedTime);
        return dataAnswer;
    }
}
