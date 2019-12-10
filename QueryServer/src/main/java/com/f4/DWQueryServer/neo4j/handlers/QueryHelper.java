package com.f4.DWQueryServer.neo4j.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.ResultSummary;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

class QueryHelper {
    static DataAnswer getDataAndTime(String queryTemplate, Driver driver) {
        long time;
        List<String> answer = new ArrayList<>();
        SimpleDateFormat format = new SimpleDateFormat("YYYY-mm-dd");
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
                    answer.add(format.format(date));
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

    static TestAnswer getTime(String queryTemplate,Driver driver) {
        long time;
        try(Session session = driver.session()) {
            StatementResult result = session.run(queryTemplate);
            ResultSummary summary = result.summary();
            time = summary.resultAvailableAfter(TimeUnit.MILLISECONDS);
        }
        TestAnswer answer = new TestAnswer();
        answer.setTime(time);
        return answer;
    }
}
