package com.f4.DWQueryServer.neo4j.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import org.neo4j.driver.v1.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class Query_7 {
    @Autowired
    Driver driver;
    // search on the user's comment
    // match (m:MOVIE) where m.commentNumber <> 0 set m.score = m.totalScore/m.commentNumber
    private String getQueryString(SpecificQuery query){
        double from = query.getComment().getScore_from();
        double to = query.getComment().getScore_to();
        String queryTemplate = "";
        switch (query.getAnswer()) {
            case "count": {
                queryTemplate = "match (m:MOVIE) where m.score >= " + from + " and m.score <= " + to +" return count(m) as count";
            }
            break;
            case "title": {
                queryTemplate = "match (m:MOVIE) where m.score >= " + from + " and m.score <= " + to +" return m.name as name";
            }
            break;
            case "actor": {
                queryTemplate = "match (m:MOVIE)<-[:ACT_IN]-(a:ACTOR) where m.score >= "  + from + " and m.score <= " + to +" return distinct a.name as name" ;
            }
            break;
            case "id": {
                queryTemplate = "match (m:MOVIE)<-[:IDENTIFIES]-(id:MOVIE_ID) where  m.score >= "  + from + " and m.score <= " + to +" return id.id as id";
            }
            break;
            case "director": {
                queryTemplate = "match (m:MOVIE)<-[:DIRECTED]-(d:DIRECTOR) where  m.score >= "  + from + " and m.score <= " + to +" return distinct d.name as name";
            }
            break;
            case "date": {
                queryTemplate = "match (m:MOVIE) where m.score >= " + from + " and m.score <= " + to +" return m.releaseDate as releaseDate";
            }
            break;
            case "type": {
                queryTemplate = "match (m:MOVIE)-[:CATEGORY_IN]->(t:TYPE) where  m.score >= "  + from + " and m.score <= " + to +" return distinct t.name as name";
            }
            break;
            case "version": {
                queryTemplate = "match (m:MOVIE)-[:DELIVER_IN]->(v:VERSION) where  m.score >= "  + from + " and m.score <= " + to +" return distinct v.name as name";
            }
            break;
            case "comment": {
                queryTemplate = "match (m:MOVIE)<-[:COMMENT_ON]-(c:COMMENT) where  m.score >= "  + from + " and m.score <= " + to +" return c.summary as comment";
            }
        }
        return queryTemplate;
    }

    private String getUserCommentQueryString(SpecificQuery query){
        String userName = query.getComment().getUser_name();
        double from = query.getComment().getScore_from();
        double to = query.getComment().getScore_to();
        String userID = query.getComment().getUser_id();
        String queryTemplate = "";
        if ("title".equals(query.getAnswer())) {
            queryTemplate = "match(u:USER{profile_name:'" + userName + "'})-[g:GRADE]->(m:MOVIE) where g.point >= " + from + " and g.point <= " + to + " return m.name as name";
        }
        if("count".equals(query.getAnswer())){
            queryTemplate = "match(u:USER{profile_name:'" + userName + "'})-[g:GRADE]->(m:MOVIE) where g.point >= " + from + " and g.point <= " + to + " return count(m) as count";
        }

        return queryTemplate;
    }

    public DataAnswer getDataAnswer(SpecificQuery query){
        String queryTemplate = "";
        if(query.getComment().getUser_name() != null){
            queryTemplate = getUserCommentQueryString(query);
        }else {
            queryTemplate = getQueryString(query);
        }
        return QueryHelper.getDataAndTime(queryTemplate,driver);
    }

    public TestAnswer getTestAnswer(SpecificQuery query){
        String queryTemplate = "";
        queryTemplate = getQueryString(query);
        return QueryHelper.getTime(queryTemplate,driver);
    }
}
