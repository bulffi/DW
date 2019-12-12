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
 * @CreateDate: Created in 2019/12/11 16:19 <br>
 * @Author: <a href="abc@qq.com">abc</a>
 */

@Service
public class Query4 { //按导演查询
    @Autowired
    Connection connection;

    public DataAnswer getDataAnswer(SpecificQuery query) throws SQLException {
        //要用的数据
        List<String> actors = query.getActors();
        String main_actor = query.getMain_actor();
        int num_actors = actors.size();
        String answer = query.getAnswer();

        //初始化返回值
        DataAnswer dataAnswer = new DataAnswer();
        List<String> data = new ArrayList<>();

        if(num_actors == 0 && main_actor.equals("")){ //没有指定演员或主演
            data.add("请指定演员或主演");
            dataAnswer.setData(data);
            dataAnswer.setTime(Long.valueOf(0));
            return dataAnswer;
        }

//        select temp1.movie_id from
//            (select movie_id
//            from movie_director
//            where director_name = "Tomomi Mochizuki") as temp1
//            inner join
//            (select movie_id
//            from movie_director b
//            where director_name = "Shinji Takagi") as temp2
//            on temp1.movie_id = temp2.movie_id
//            inner join
//            (select movie_id
//            from movie_director b
//            where director_name = "Takeshi Mori") as temp3
//            on temp2.movie_id = temp3.movie_id;
        //for循环拼接sql语句
        StringBuffer sql = new StringBuffer();
        sql.append("select movie_title from movie_info_fact where movie_id in (select temp0.movie_id from ");
        int i = 0;
        for(; i < num_actors; i++){
            if(i != 0) {//不是第一个
                sql.append("inner join ");
            }
            sql.append("(select movie_id from movie_actor where actor = \"" + actors.get(i) + "\") as temp" + i + ' ');
            if(i != 0) {//不是第一个
                sql.append("on temp" + (i - 1) + ".movie_id = temp" + i + ".movie_id ");
            }
        }
        if(main_actor.equals("") && num_actors != 0){//没有主演但有演员列表
            sql.append(')');
        }
        else if (!main_actor.equals("") && num_actors != 0){ //既有演员列表又有主演
            sql.append("inner join (select movie_id from movie_actor where is_lead = 1 and actor = \"" + main_actor + "\") as temp" + i + " on temp" + (i-1) + ".movie_id = temp" + i + ".movie_id)");
        }
        else { //没有演员列表但又主演
            sql.append("(select movie_id from movie_actor where is_lead = 1 and actor = \"" + main_actor + "\") as temp0)");
        }

        System.out.println(sql);
        PreparedStatement preparedStatement = connection.prepareStatement(String.valueOf(sql));//声明查询语句
        //执行sql语句并计时
        long startTime =  System.currentTimeMillis();//开始计时
        ResultSet resultSet = preparedStatement.executeQuery();//执行sql
        long endTime =  System.currentTimeMillis();//结束计时
        long usedTime = endTime-startTime;//计算耗时(单位为毫秒)

        dataAnswer.setTime(usedTime);
        while (resultSet.next()){
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