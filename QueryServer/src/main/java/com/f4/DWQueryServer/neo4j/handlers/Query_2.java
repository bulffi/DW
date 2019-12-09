package com.f4.DWQueryServer.neo4j.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.neo4j.driver.v1.Values.parameters;

@Service
public class Query_2 {
    @Autowired
    Driver driver;
    public DataAnswer getDataAnswer(SpecificQuery query){
        //Completed after 273718 ms.
        // with out index
        // 1063 ms with index
        String queryTemplate="";
        switch (query.getAnswer()){
            case "count":{
                queryTemplate = "match(m:MOVIE) where m.name=~ '(.*?)" + query.getMovie_name() + "(.*?)' return count(m) as count";
            }break;
            case "title" :{
                queryTemplate = "match(m:MOVIE) where m.name=~ '(.*?){name}(.*?)' return m.name as name";
            }break;
            case "actor":{
                queryTemplate = "match(m:MOVIE)<-[:ACT_IN]-(a:ACTOR) where m.name=~ '(.*?){name}(.*?)' return a.name as name";
            }break;
            case "id":{
                queryTemplate = "match(m:MOVIE)<-[:IDENTIFIES]-(id:MOVIE_ID) where m.name=~ '(.*?)$name(.*?)' return id";
            }break;
            case "director":{
                queryTemplate = "match(m:MOVIE)<-[:DIRECTED]-(d:DIRECTOR) where m.name=~ '(.*?)$name(.*?)' return d.name as name";
            }break;
            case "date":{
                queryTemplate = "match(m:MOVIE) where m.name=~ '(.*?)$name(.*?)' return m.releaseDate as releaseDate";
            }break;
            case "type":{
                queryTemplate = "match(m:MOVIE)-[:CATEGORY_IN]->(t:TYPE) where m.name=~ '(.*?)$name(.*?)' return t.name as name";
            }break;
            case "version":{
                queryTemplate = "match(m:MOVIE)-[:DELIVER_IN]->(v:VERSION) where m.name=~ '(.*?)$name(.*?)' return v.name as name";
            }break;
            case "comment":{
                queryTemplate = "match(m:MOVIE)<-[:COMMENT_ON]-(c:COMMENT) where m.name=~ '(.*?)$name(.*?)' return comment.summary as comment";
            }

        }
        List<String> answer = new ArrayList<>();
        long time;
        try(Session session = driver.session()) {
            StatementResult result = session.run(queryTemplate);
            ResultSummary summary = result.summary();
            System.out.println(summary.statement().toString());
            time = summary.resultAvailableAfter(TimeUnit.MILLISECONDS);
            while (result.hasNext()){
                Record record = result.next();
                if(record.containsKey("count")){
                    answer.add(record.get("count").toString());
                }else if(record.containsKey("name")){
                    answer.add(record.get("name").asString());
                }else if(record.containsKey("releaseDate")){
                    Date date = new Date(record.get("releaseDate").asLong());
                    answer.add(date.toString());
                }else if(record.containsKey("id")){
                    answer.add(record.get("id").asString());
                }else if(record.containsKey("comment")){
                    answer.add(record.get("comment").asString());
                }
            }
        }
        DataAnswer dataAnswer = new DataAnswer();
        dataAnswer.setData(answer);
        dataAnswer.setTime(time);
        return dataAnswer;
    }
}
