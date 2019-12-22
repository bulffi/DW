package com.f4.DWQueryServer.hbase;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
public class HbaseQuerierHandler {
    @Resource
    HbaseQuerier querier;

    public DataAnswer queryByDateForData(SpecificQuery specificQuery){
        SpecificQuery.DWTime timeFrom = specificQuery.getTime_from();
        int year = timeFrom.getYear();//年份
        int month = timeFrom.getMonth();//月份
        int quarter = timeFrom.getQuarter();//季度
        int day = timeFrom.getDay_of_week();

        String[] weekday = {"Mon", "Tue", "Wed","Thi","Fri","Sat","Sun"};

        if(day > 0 && day < 8){
            return querier.queryByWeekday(weekday[day - 1]);
        }
        if(quarter > 0){
            return querier.queryBySeason(year, quarter);
        }
        return querier.queryByTime(year, month);
    }

    public TestAnswer queryByDateForTest(SpecificQuery specificQuery){
        TestAnswer testAnswer = new TestAnswer();
        testAnswer.setTime(queryByDateForData(specificQuery).getTime());
        return testAnswer;
    }



    public DataAnswer queryByTypeForData(SpecificQuery specificQuery){
        List<String> types = specificQuery.getType();
        if(types.size() == 0){
            return new DataAnswer();
        }

        return querier.queryByType(types.get(0));
    }

    public TestAnswer queryByTypeForTest(SpecificQuery specificQuery){
        TestAnswer testAnswer = new TestAnswer();
        testAnswer.setTime(queryByDateForData(specificQuery).getTime());
        return testAnswer;
    }




    public DataAnswer queryByActorForData(SpecificQuery specificQuery){
        List<String> actors = specificQuery.getActors();
        if(actors == null || actors.size() == 0){
            return new DataAnswer();
        }
        return querier.queryByActor(actors.get(0));
    }

    public TestAnswer queryByActorForTest(SpecificQuery specificQuery){
        TestAnswer testAnswer = new TestAnswer();
        testAnswer.setTime(queryByActorForData(specificQuery).getTime());
        return testAnswer;
    }




    public DataAnswer queryByDirectorForData(SpecificQuery specificQuery){
        List<String> director_names = specificQuery.getDirectors();
        if(director_names.size() == 0){
            return new DataAnswer();
        }
        String director = director_names.get(0);
        return querier.queryByDirector(director);
    }

    public TestAnswer queryByDirectorForTest(SpecificQuery specificQuery){
        TestAnswer testAnswer = new TestAnswer();
        testAnswer.setTime(queryByDirectorForData(specificQuery).getTime());
        return testAnswer;
    }



    public DataAnswer queryByTitleForData(SpecificQuery specificQuery){
        String title = specificQuery.getMovie_name();
        String answer = specificQuery.getAnswer();

        return querier.queryByTitle(title);
    }

    public TestAnswer queryByTitleForTest(SpecificQuery specificQuery){
        TestAnswer testAnswer = new TestAnswer();
        testAnswer.setTime(queryByTitleForData(specificQuery).getTime());
        return testAnswer;
    }


    public DataAnswer queryByUserForData(SpecificQuery specificQuery){
        String userName = specificQuery.getComment().getUser_name();
        double score_from = specificQuery.getComment().getScore_from();
        double score_to = specificQuery.getComment().getScore_to();

        return querier.queryByUser(userName, score_from, score_to, 2);
    }

    public TestAnswer queryByUserForTest(SpecificQuery specificQuery){
        TestAnswer testAnswer = new TestAnswer();
        testAnswer.setTime(queryByUserForData(specificQuery).getTime());
        return testAnswer;
    }
}
