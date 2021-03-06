package com.f4.DWQueryServer.neo4j.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.CollaborateQuery;
import lombok.Data;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class Query_5 {
    @Autowired
    Driver driver;
    // query on collaboration
    private String getQueryString(CollaborateQuery query){
        String queryString = "";
        switch (query.getType()){
            case 1:{
                queryString = "match (a1:ACTOR)-[:ACT_IN]->(m:MOVIE)<-[:ACT_IN]-(a2:ACTOR) " +
                        "with a1,a2,count(m) as num where num > " + query.getThreshold() + " return a1.name as name1,a2.name as name2,num order by num desc";
            }break;
            case 2:{
                queryString  = "match (a:ACTOR)-[:ACT_IN]->(m:MOVIE)<-[:DIRECTED]-(d:DIRECTOR) " +
                        "with a,d,count(m) as num where num >" + query.getThreshold() + " return a.name as name1,d.name as name2,num order by num desc";
            }break;
            case 3:{
                queryString = "match (d1:DIRECTOR)-[:DIRECTED]->(m:MOVIE)<-[:DIRECTED]-(d2:DIRECTOR) " +
                        "with d1,d2,count(m) as num where num >" +query.getThreshold() +" return d1.name as name1,d2.name as name2,num order by num desc";
            }
        }
        return queryString;
    }
    @Data
    static class Pair{
        String name_1;
        String name_2;
        int num;
    }

    public DataAnswer getDataAnswer(CollaborateQuery query){
        String queryTemplate = getQueryString(query);
        List<String> answer = new ArrayList<>();
        Set<Pair> pairs = new HashSet<>();
        try (Session session = driver.session()){
            StatementResult result = session.run(queryTemplate);
            ResultSummary summary = result.summary();
            System.out.println(summary.statement().toString());
            long time = summary.resultAvailableAfter(TimeUnit.MILLISECONDS);
            while (result.hasNext()){
                Record record = result.next();
                String name1 = record.get("name1").asString();
                String name2 = record.get("name2").asString();
                int num = record.get("num").asInt();
                Pair pair = new Pair();
                pair.setName_1(name1);
                pair.setName_2(name2);
                pair.setNum(num);
                boolean isNew = true;
                for (Pair p: pairs) {
                    if((p.getName_1().equals(pair.getName_1()) && p.getName_2().equals(pair.getName_2()) ||
                            p.getName_1().equals(pair.getName_2()) && p.getName_2().equals(pair.getName_1()))){
                        isNew = false;
                        break;
                    }
                }
                if(isNew){
                    pairs.add(pair);
                }
            }
            List<Pair> pairList = new ArrayList<>(pairs);
            pairList.sort((o1, o2) -> Integer.compare(o2.getNum(), o1.getNum()));
            for (Pair p : pairList) {
                answer.add(p.getName_1() + " & " + p.getName_2() + " for " + p.getNum() + "time(s)");
            }
            DataAnswer dataAnswer = new DataAnswer();
            dataAnswer.setTime(time);
            dataAnswer.setData(answer);
            return dataAnswer;
        }
    }

    TestAnswer getTestAnswer(CollaborateQuery query){
        String queryString = getQueryString(query);
        return QueryHelper.getTime(queryString,driver);
    }

}
