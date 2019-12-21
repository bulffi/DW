package com.f4.DWQueryServer.neo4j.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import org.neo4j.driver.v1.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class Cypher {
    @Autowired
    Driver driver;
    public DataAnswer getDataAnswer(String queryString){
        return QueryHelper.getDataAndTime(queryString,driver);
    }
}
