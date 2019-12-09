package com.f4.DWQueryServer.neo4j.handlers;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
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
    public DataAnswer getDataAnswer(SpecificQuery query) {
        SpecificQuery.DWTime timeFrom = query.getTime_from();
        SpecificQuery.DWTime timeTo = query.getTime_to();
        Date dateFrom = new Date();
        Date dateTo = new Date();
        if (timeFrom.getYear() == 0) {


        } else {
            // has year info

        }
        return new DataAnswer();
    }
}
