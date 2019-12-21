package com.f4.DWQueryServer.mysql;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.CollaborateQuery;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import com.f4.DWQueryServer.mysql.handlers.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;

/**
 * @program: DW
 * @description:
 * @author: Zijian Zhang
 * @create: 2019/12/04
 **/
@RestController
public class MySQLMainHandler {
    public static boolean optimize = true;

    @Autowired
    Query1 query1;
    @Autowired
    Query2 query2;
    @Autowired
    Query3 query3;
    @Autowired
    Query4 query4;
    @Autowired
    Query6 query6;
    @Autowired
    Query7 query7;

    @Autowired
    Query5 query5;

    @PostMapping("/MySQL/specify")
    public Object handleSpecifyQuery(@RequestBody SpecificQuery specificQuery) throws SQLException {
        // System.out.println(specificQuery.getTime_from().getDay_of_month());
        if(specificQuery.getIdList().size() == 1) { //IdList不为空
            String queryID = specificQuery.getIdList().get(0);
            switch (queryID) { //目前只支持一个queryID的查询
                case "1":{
                    if(specificQuery.getAnswerType().equals("data")){
                        return query1.getDataAnswer(specificQuery);
                    }else {
                        return query1.getTestAnswer(specificQuery);
                    }
                }
                case "2":{
                    if(specificQuery.getAnswerType().equals("data")){
                        return query2.getDataAnswer(specificQuery);
                    }else {
                        return query2.getTestAnswer(specificQuery);
                    }
                }
                case "3":{
                    if(specificQuery.getAnswerType().equals("data")){
                        return query3.getDataAnswer(specificQuery);
                    }else {
                        return query3.getTestAnswer(specificQuery);
                    }
                }
                case "4":{
                    if(specificQuery.getAnswerType().equals("data")){
                        return query4.getDataAnswer(specificQuery);
                    }else {
                        return query4.getTestAnswer(specificQuery);
                    }
                }
                case "6":{
                    if(specificQuery.getAnswerType().equals("data")){
                        return query6.getDataAnswer(specificQuery);
                    }else {
                        return query6.getTestAnswer(specificQuery);
                    }
                }
                case "7":{
                    if(specificQuery.getAnswerType().equals("data")){
                        return query7.getDataAnswer(specificQuery);
                    }else {
                        return query7.getTestAnswer(specificQuery);
                    }
                }
            }
        }
        //IdList为空，返回空查询结果
        return new DataAnswer();
    }
    @PostMapping("/MySQL/general/collaboration")
    public Object handleCollaborationQuery(@RequestBody CollaborateQuery collaborateQuery) throws SQLException {

        return query5.getDataAnswer(collaborateQuery);
    }
}
