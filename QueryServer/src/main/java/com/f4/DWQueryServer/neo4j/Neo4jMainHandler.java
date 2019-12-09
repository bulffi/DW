package com.f4.DWQueryServer.neo4j;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.ClientQuery;
import com.f4.DWQueryServer.entity.query.CollaborateQuery;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import com.f4.DWQueryServer.neo4j.handlers.Query_1;
import com.f4.DWQueryServer.neo4j.handlers.Query_2;
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
    @PostMapping("/Neo4j/specify")
    public Object handleSpecifyQuery(@RequestBody SpecificQuery specificQuery){
        if(specificQuery.getIdList().size() == 1){
            if(specificQuery.getIdList().get(0).equals("2")){
                if(specificQuery.getAnswerType().equals("data")){
                    return query_2.getDataAnswer(specificQuery);
                }
            }
        }
        return new DataAnswer();
    }
    @PostMapping("/Neo4j/general/collaboration")
    public Object handleCollaborationQuery(@RequestBody CollaborateQuery collaborateQuery){
        return new TestAnswer();
    }
    @PostMapping("/Neo4j/general/similarUser")
    public Object handleSimilarUserQuery(@RequestBody ClientQuery clientQuery){
        return new DataAnswer();
    }
}
