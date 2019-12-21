package com.f4.DWQueryServer.neo4j.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.query.ClientQuery;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class Query_8 {
    @Autowired
    Driver driver;

    public DataAnswer getDataAnswer(ClientQuery query) {
        DataAnswer dataAnswer = new DataAnswer();
        String queryString = "match(m:MOVIE) where m.commentNumber>900  match(u1:USER)-[:LIKES]->(m)<-[:LIKES]-(u2:USER) return u1.profile_name as name1,u2.profile_name as name2,count(m) order by count(m) desc limit 50 ";
        List<String> answer = new ArrayList<>();
        Set<Query_5.Pair> pairs = new HashSet<>();
        try (Session session = driver.session()) {
            StatementResult result = session.run(queryString);
            ResultSummary summary = result.summary();
            System.out.println(summary.statement().toString());
            long time = summary.resultAvailableAfter(TimeUnit.MILLISECONDS);
            while (result.hasNext()) {
                Record record = result.next();
                String name1 = record.get("name1").asString();
                String name2 = record.get("name2").asString();
                int num = record.get("count(m)").asInt();
                Query_5.Pair pair = new Query_5.Pair();
                pair.setName_1(name1);
                pair.setName_2(name2);
                pair.setNum(num);
                boolean isNew = true;
                for (Query_5.Pair p : pairs) {
                    if ((p.getName_1().equals(pair.getName_1()) && p.getName_2().equals(pair.getName_2()) ||
                            p.getName_1().equals(pair.getName_2()) && p.getName_2().equals(pair.getName_1()))) {
                        isNew = false;
                        break;
                    }
                }
                if (isNew) {
                    pairs.add(pair);
                }
            }
            for (Query_5.Pair p : pairs) {
                answer.add(p.getName_1() + " & " + p.getName_2() + " for " + p.getNum() + "time(s)");
            }
            dataAnswer.setTime(time);
            dataAnswer.setData(answer);
            return dataAnswer;
        }
    }
}
