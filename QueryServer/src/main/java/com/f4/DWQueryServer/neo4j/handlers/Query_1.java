package com.f4.DWQueryServer.neo4j.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import org.neo4j.driver.v1.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

@Service
public class Query_1 {
    @Autowired
    Driver driver;
    private String getQueryString(SpecificQuery query){
        SpecificQuery.DWTime timeFrom = query.getTime_from();
        String queryTemplate = "";
        String matchingClause = "match ";
        if(timeFrom.getYear() != 0){
            matchingClause += "(y:YEAR{num: " + timeFrom.getYear() +" })-[:OF]->";
        }
        if(timeFrom.getQuarter() != 0){
            matchingClause += "(q:QUARTER{num: "+ timeFrom.getQuarter() +" })-[:OF]->";
        }
        if(timeFrom.getMonth() != 0){
            matchingClause +="(m:MONTH{num: "+ timeFrom.getMonth() +" })-[:OF]->";
        }
        if(timeFrom.getDay_of_week() != 0){
            matchingClause +="(dow:DAY_OF_WEEK{num: "+ timeFrom.getDay_of_week() +" })-[:OF]->";
        }
        if(timeFrom.getDay_of_month() != 0){
            matchingClause +="(dom:DAY_OF_MONTH{num: "+ timeFrom.getDay_of_month() +"})-[:OF]->";
        }
        matchingClause += "(movie:MOVIE)";
        queryTemplate = matchingClause;
        switch (query.getAnswer()) {
            case "count": {
                queryTemplate += " return count(movie) as count";
            }
            break;
            case "title": {
                queryTemplate += " return movie.name as name";
            }
            break;
            case "actor": {
                queryTemplate += "<-[:ACT_IN]-(a:ACTOR) return distinct a.name as name";
            }
            break;
            case "id": {
                queryTemplate += "<-[:IDENTIFIES]-(id:MOVIE_ID) return distinct id.id as id";
            }
            break;
            case "director": {
                queryTemplate += "<-[:DIRECTED]-(d:DIRECTOR) return distinct d.name as name";
            }
            break;
            case "date": {
                queryTemplate += "return movie.releaseDate as releaseDate";
            }
            break;
            case "type": {
                queryTemplate += "-[:CATEGORY_IN]->(t:TYPE) return distinct t.name as name";
            }
            break;
            case "version": {
                queryTemplate += "-[:DELIVERED_IN]->(v:VERSION) return distinct v.name as name";
            }
            break;
            case "comment": {
                queryTemplate += "<-[:COMMENT_ON]-(co:COMMENT) return distinct co.summary as comment";
            }
        }
        return queryTemplate;
    }
    public DataAnswer getDataAnswer(SpecificQuery query){
        //Completed after 273718 ms.
        // with out index
        // 1063 ms with index
        String name = query.getMovie_name();
        String queryTemplate = getQueryString(query);
        return QueryHelper.getDataAndTime(queryTemplate,driver);
    }
    public TestAnswer getTestAnswer(SpecificQuery query){
        String name = query.getMovie_name();
        String queryTemplate = getQueryString(query);
        return QueryHelper.getTime(queryTemplate,driver);
    }
}
