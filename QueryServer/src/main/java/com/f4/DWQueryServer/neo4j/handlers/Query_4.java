package com.f4.DWQueryServer.neo4j.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import org.neo4j.driver.v1.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class Query_4 {
    @Autowired
    Driver driver;
    private String getQueryTemplate(SpecificQuery query,String name,String relation){
        // relation only ACT_IN or MAIN_ACT_IN
        assert relation.equals("ACT_IN") || relation.equals("MAIN_ACT_IN");
        String queryTemplate = "";
        switch (query.getAnswer()){
            case "count": {
                queryTemplate = "match (a:ACTOR)-[:" + relation + "]->(m:MOVIE) where a.name=~'(.*?)" + name + "(.*?)' return count(m) as count";
            }
            break;
            case "title": {
                queryTemplate = "match (a:ACTOR)-[:" + relation + "]->(m:MOVIE) where a.name=~'(.*?)" + name + "(.*?)' return m.name as name";
            }
            break;
            case "actor": {
                queryTemplate = "match (a1:ACTOR)-[:" + relation + "]->(m:MOVIE)<-[:ACT_IN]-(a2:ACTOR) where a1.name=~'(.*?)" + name + "(.*?)' return distinct a2.name as name";
            }
            break;
            case "id": {
                queryTemplate = "match (a:ACTOR)-[:" + relation + "]->(m:MOVIE)<-[:IDENTIFIES]-(id:MOVIE_ID) where a.name=~'(.*?)" + name + "(.*?)' return id.id as id";
            }
            break;
            case "director": {
                queryTemplate = "match (a:ACTOR)-[:" + relation + "]->(m:MOVIE)<-[:DIRECTED]-(d:DIRECTOR) where a.name=~'(.*?)" + name + "(.*?)' return distinct d.name as name";
            }
            break;
            case "date": {
                queryTemplate = "match (a:ACTOR)-[:" + relation + "]->(m:MOVIE) where a.name=~'(.*?)" + name + "(.*?)' return m.releaseDate as releaseDate";
            }
            break;
            case "type": {
                queryTemplate = "match (a:ACTOR)-[:" + relation + "]->(m:MOVIE)-[:CATEGORY_IN]->(t:TYPE) where a.name=~'(.*?)" + name + "(.*?)' return distinct t.name as name";
            }
            break;
            case "version": {
                queryTemplate = "match (a:ACTOR)-[:" + relation + "]->(m:MOVIE)-[:DELIVER_IN]->(v:VERSION) where a.name=~'(.*?)" + name + "(.*?)' return distinct v.name as name";
            }
            break;
            case "comment": {
                queryTemplate = "match (a:ACTOR)-[:" + relation + "]->(m:MOVIE)<-[:COMMENT_ON]-(c:COMMENT) where a.name=~'(.*?)" + name + "(.*?)' return distinct c.summary as comment";
            }
        }
        return queryTemplate;
    }
    public DataAnswer getDataAnswer(SpecificQuery query){
        String queryTemplate = "";
        if(query.getMain_actor() != null){
            queryTemplate = getQueryTemplate(query,query.getMain_actor(),"MAIN_ACT_IN");
        }else {
            queryTemplate = getQueryTemplate(query,query.getActors().get(0),"ACT_IN");
        }
        return QueryHelper.getDataAndTime(queryTemplate,driver);
    }


    public TestAnswer getTestAnswer(SpecificQuery query){
        String queryTemplate = "";
        if(query.getMain_actor() != null){
            queryTemplate = getQueryTemplate(query,query.getMain_actor(),"MAIN_ACT_IN");
        }else {
            queryTemplate = getQueryTemplate(query,query.getActors().get(0),"ACT_IN");
        }
        return QueryHelper.getTime(queryTemplate,driver);
    }
}
