package com.f4.DWQueryServer.neo4j.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class Query_2 {
    @Autowired
    Driver driver;
    private String getString(SpecificQuery query, String name) {
        String queryTemplate = "";
        switch (query.getAnswer()) {
            case "count": {
                queryTemplate = "match(m:MOVIE) where m.name=~ '(.*?)" + query.getMovie_name() + "(.*?)' return count(m) as count";
            }
            break;
            case "title": {
                queryTemplate = "match(m:MOVIE) where m.name=~ '(.*?)" + name + "(.*?)' return m.name as name";
            }
            break;
            case "actor": {
                queryTemplate = "match(m:MOVIE)<-[:ACT_IN]-(a:ACTOR) where m.name=~ '(.*?)" + name + "(.*?)' return a.name as name";
            }
            break;
            case "id": {
                queryTemplate = "match(m:MOVIE)<-[:IDENTIFIES]-(id:MOVIE_ID) where m.name=~ '(.*?)" + name + "(.*?)' return id.id as id";
            }
            break;
            case "director": {
                queryTemplate = "match(m:MOVIE)<-[:DIRECTED]-(d:DIRECTOR) where m.name=~ '(.*?)" + name + "(.*?)' return d.name as name";
            }
            break;
            case "date": {
                queryTemplate = "match(m:MOVIE) where m.name=~ '(.*?)" + name + "(.*?)' return m.releaseDate as releaseDate";
            }
            break;
            case "type": {
                queryTemplate = "match(m:MOVIE)-[:CATEGORY_IN]->(t:TYPE) where m.name=~ '(.*?)" + name + "(.*?)' return t.name as name";
            }
            break;
            case "version": {
                queryTemplate = "match(m:MOVIE)-[:DELIVER_IN]->(v:VERSION) where m.name=~ '(.*?)" + name + "(.*?)' return v.name as name";
            }
            break;
            case "comment": {
                queryTemplate = "match(m:MOVIE)<-[:COMMENT_ON]-(c:COMMENT) where m.name=~ '(.*?)" + name + "(.*?)' return c.summary as comment";
            }

        }
        return queryTemplate;
    }
    public DataAnswer getDataAnswer(SpecificQuery query){
        //Completed after 273718 ms.
        // with out index
        // 1063 ms with index
        String name = query.getMovie_name();
        String queryTemplate = getString(query, name);
        return QueryHelper.getDataAndTime(queryTemplate,driver);
    }
    public TestAnswer getTestAnswer(SpecificQuery query){
        String name = query.getMovie_name();
        String queryTemplate = getString(query, name);
        return QueryHelper.getTime(queryTemplate,driver);
    }

}
