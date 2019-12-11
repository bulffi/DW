package com.f4.DWQueryServer.mysql;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.answer.TestAnswer;
import com.f4.DWQueryServer.entity.query.CollaborateQuery;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @program: DW
 * @description:
 * @author: Zijian Zhang
 * @create: 2019/12/04
 **/
@RestController
public class MySQLMainHandler {
    @PostMapping("/MySQL/specify")
    public Object handleSpecifyQuery(@RequestBody SpecificQuery specificQuery){
        System.out.println(specificQuery.getTime_from().getDay_of_month());
        return new DataAnswer();
    }
    @PostMapping("/MySQL/general/collaboration")
    public Object handleCollaborationQuery(@RequestBody CollaborateQuery collaborateQuery){
        return new TestAnswer();
    }
}
