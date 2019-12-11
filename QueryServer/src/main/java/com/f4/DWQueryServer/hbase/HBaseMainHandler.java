package com.f4.DWQueryServer.hbase;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.query.CollaborateQuery;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
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
public class HBaseMainHandler {
    @PostMapping("/HBase/specify")
    public Object handleSpecifyQuery(@RequestBody SpecificQuery specificQuery){
        return new DataAnswer();
    }
    @PostMapping("/HBase/general/collaboration")
    public Object handleCollaborationQuery(@RequestBody CollaborateQuery collaborateQuery){
        return  new DataAnswer();
    }
}
