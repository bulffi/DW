package com.f4.DWQueryServer.neo4j.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import org.neo4j.driver.v1.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class Query_6 {
    // query on the type of the movie
    @Autowired
    Driver driver;
    private String getQueryString(SpecificQuery query, String name){
        String queryTemplate = "";
        switch (query.getAnswer()) {
            case "count": {
                queryTemplate = "match (t:TYPE)<-[:CATEGORY_IN]-(m:MOVIE) where t.name =~ '(.*?)" + name + "(.*?)' return count(m) as count";
            }
            break;
            case "title": {
                queryTemplate = "match (t:TYPE)<-[:CATEGORY_IN]-(m:MOVIE) where t.name =~ '(.*?)" + name + "(.*?)' return m.name as name";
            }
            break;
            case "actor": {
                queryTemplate = "match(t:TYPE)<-[:CATEGORY_IN]-(m:MOVIE)<-[:ACT_IN]-(a:ACTOR) where t.name=~ '(.*?)" + name + "(.*?)' return distinct a.name as name";
            }
            break;
            case "id": {
                queryTemplate = "match(t:TYPE)<-[:CATEGORY_IN]-(m:MOVIE)<-[:IDENTIFIES]-(id:MOVIE_ID) where t.name =~ '(.*?)" + name + "(.*?)' return id.id as id";
            }
            break;
            case "director": {
                queryTemplate = "match(t:TYPE)<-[:CATEGORY_IN]-(m:MOVIE)<-[:DIRECTED]-(d:DIRECTOR) where t.name =~ '(.*?)" + name + "(.*?)' return distinct d.name as name";
            }
            break;
            case "date": {
                queryTemplate = "match(t:TYPE)<-[:CATEGORY_IN]-(m:MOVIE)where t.name =~ '(.*?)" + name + "(.*?)' return m.releaseDate as releaseDate";
            }
            break;
            case "type": {
                queryTemplate = "match(t1:TYPE)<-[:CATEGORY_IN]-(m:MOVIE)-[:CATEGORY_IN]->(t2:TYPE) where t1.name =~ '(.*?)" + name + "(.*?)' return distinct t2.name as name";
            }
            break;
            case "version": {
                queryTemplate = "match(t:TYPE)<-[:CATEGORY_IN]-(m:MOVIE)-[:DELIVER_IN]->(v:VERSION) where t.name =~ '(.*?)" + name + "(.*?)' return distinct v.name as name";
            }
            break;
            case "comment": {
                queryTemplate = "match(t:TYPE)<-[:CATEGORY_IN]-(m:MOVIE)<-[:COMMENT_ON]-(c:COMMENT) where t.name =~ '(.*?)" + name + "(.*?)' return c.summary as comment";
            }
        }
        return queryTemplate;
    }
    public DataAnswer getDataAnswer(SpecificQuery query){
        String queryTemplate = "";
        queryTemplate = getQueryString(query,query.getType().get(0));
        return QueryHelper.getDataAndTime(queryTemplate,driver);
    }


    public TestAnswer getTestAnswer(SpecificQuery query){
        String queryTemplate = "";
        queryTemplate = getQueryString(query,query.getType().get(0));
        return QueryHelper.getTime(queryTemplate,driver);
    }
}
