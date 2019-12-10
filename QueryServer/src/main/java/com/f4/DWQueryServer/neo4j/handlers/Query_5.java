package com.f4.DWQueryServer.neo4j.handlers;

import com.f4.DWQueryServer.entity.query.CollaborateQuery;
import org.neo4j.driver.v1.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class Query_5 {
    @Autowired
    Driver driver;
    // query on collaboration
    private String getQueryString(CollaborateQuery query){
        String queryString = "";
        switch (query.getType()){
            case 1:{
                queryString = "";
            }break;
            case 2:{
                queryString  = "";
            }break;
            case 3:{
                queryString = "";
            }
        }
        return queryString;
    }

}
