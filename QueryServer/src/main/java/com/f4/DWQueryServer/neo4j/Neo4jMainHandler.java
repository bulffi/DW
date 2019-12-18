package com.f4.DWQueryServer.neo4j;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.ClientQuery;
import com.f4.DWQueryServer.entity.query.CollaborateQuery;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import com.f4.DWQueryServer.neo4j.handlers.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @program: DW
 * @description:
 * @author: Zijian Zhang
 * @create: 2019/12/04
 **/
@RestController
public class Neo4jMainHandler {
    @Autowired
    Query_1 query_1;
    @Autowired
    Query_2 query_2;
    @Autowired
    Query_3 query_3;
    @Autowired
    Query_4 query_4;
    @Autowired
    Query_5 query_5;
    @Autowired
    Query_6 query_6;
    @Autowired
    Query_7 query_7;
    @PostMapping("/Neo4j/specify")
    public Object handleSpecifyQuery(@RequestBody SpecificQuery specificQuery){
        if(specificQuery.getIdList().size() == 1){
            String queryID = specificQuery.getIdList().get(0);
            switch (queryID){
                case "1":{
                    if(specificQuery.getAnswerType().equals("data")){
                        return query_1.getDataAnswer(specificQuery);
                    }else {
                        return query_1.getTestAnswer(specificQuery);
                    }
                }
                case "2":{
                    if(specificQuery.getAnswerType().equals("data")){
                        return query_2.getDataAnswer(specificQuery);
                    }else {
                        return query_2.getTestAnswer(specificQuery);
                    }
                }
                case "3":{
                    if(specificQuery.getAnswerType().equals("data")){
                        return query_3.getDataAnswer(specificQuery);
                    }else {
                        return query_3.getTestAnswer(specificQuery);
                    }
                }
                case "4":{
                    if(specificQuery.getAnswerType().equals("data")){
                        return query_4.getDataAnswer(specificQuery);
                    }else {
                        return query_4.getTestAnswer(specificQuery);
                    }
                }
                case "6":{
                    if(specificQuery.getAnswerType().equals("data")){
                        return query_6.getDataAnswer(specificQuery);
                    }else {
                        return query_6.getTestAnswer(specificQuery);
                    }
                }
                case "7":{
                    if(specificQuery.getAnswerType().equals("data")){
                        return query_7.getDataAnswer(specificQuery);
                    }else {
                        return query_7.getTestAnswer(specificQuery);
                    }
                }
            }
        }
        return new DataAnswer();
    }
    @PostMapping("/Neo4j/general/collaboration")
    public Object handleCollaborationQuery(@RequestBody CollaborateQuery collaborateQuery){
        return query_5.getDataAnswer(collaborateQuery);
    }
    @PostMapping("/Neo4j/general/similarUser")
    public Object handleSimilarUserQuery(@RequestBody ClientQuery clientQuery){
        return new DataAnswer();
    }
}
