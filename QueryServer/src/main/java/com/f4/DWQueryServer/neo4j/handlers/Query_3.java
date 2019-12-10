package com.f4.DWQueryServer.neo4j.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import org.neo4j.driver.v1.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class Query_3 {
    // query by the information of the director
    @Autowired
    Driver driver;
    private String getString(SpecificQuery query, String name) {
        String queryTemplate = "";
        switch (query.getAnswer()){
            case "count":{
                queryTemplate = "match (d:DIRECTOR)-[:DIRECTED]->(m:MOVIE) where d.name =~ '(.*?)" + name +"(.*?)' return count(m) as count";
            }break;
            case "title" :{
                queryTemplate = "match (d:DIRECTOR)-[:DIRECTED]->(m:MOVIE) where d.name =~ '(.*?)" + name +"(.*?)' return m.name as name";
            }break;
            case "actor":{
                queryTemplate = "match (d:DIRECTOR)-[:DIRECTED]->(m:MOVIE)<-[:ACT_IN]-(a:ACTOR) where d.name =~ '(.*?)" + name +"(.*?)' return distinct a.name as name";
            }break;
            case "id":{
                queryTemplate = "match (d:DIRECTOR)-[:DIRECTED]->(m:MOVIE)<-[:IDENTIFIES]-(id:MOVIE_ID) where d.name =~ '(.*?)" + name +"(.*?)' return id.id as id";
            }break;
            case "director":{
                queryTemplate = "match (d1:DIRECTOR)-[:DIRECTED]->(m:MOVIE)<-[:DIRECTED]-(d2:DIRECTOR) where d1.name =~ '(.*?)" + name +"(.*?)' return distinct d2.name as name";
            }break;
            case "date":{
                queryTemplate = "match (d:DIRECTOR)-[:DIRECTED]->(m:MOVIE) where d.name =~ '(.*?)" + name +"(.*?)' return m.releaseDate as releaseDate";
            }break;
            case "type":{
                queryTemplate = "match (d:DIRECTOR)-[:DIRECTED]->(m:MOVIE)-[:CATEGORY_IN]->(t:TYPE) where d.name =~ '(.*?)" + name +"(.*?)' return distinct t.name as name";
            }break;
            case "version":{
                queryTemplate = "match (d:DIRECTOR)-[:DIRECTED]->(m:MOVIE)-[:DELIVER_IN]->(v:VERSION) where d.name =~ '(.*?)" + name +"(.*?)' return distinct v.name as name";
            }break;
            case "comment":{
                queryTemplate = "match (d:DIRECTOR)-[:DIRECTED]->(m:MOVIE)<-[:COMMENT_ON]-(c:COMMENT) where d.name =~ '(.*?)" + name +"(.*?)' return distinct c.summary as comment";
            }
        }
        return queryTemplate;
    }
    public DataAnswer getDataAnswer(SpecificQuery query){
        String name = query.getDirectors().get(0);
        String queryTemplate = getString(query, name);
        return QueryHelper.getDataAndTime(queryTemplate,driver);
    }



    public TestAnswer getTestAnswer(SpecificQuery query){
        String name = query.getDirectors().get(0);
        String queryTemplate = getString(query,name);
        return QueryHelper.getTime(queryTemplate,driver);
    }
}
