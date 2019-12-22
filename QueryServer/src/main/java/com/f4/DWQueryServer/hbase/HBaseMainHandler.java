package com.f4.DWQueryServer.hbase;


import com.f4.DWQueryServer.entity.answer.DataAnswer;
import com.f4.DWQueryServer.entity.query.CollaborateQuery;
import com.f4.DWQueryServer.entity.query.SpecificQuery;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @program: DW
 * @description:
 * @author: Zijian Zhang
 * @create: 2019/12/04
 **/
@RestController
public class HBaseMainHandler {
    @Resource
    HbaseQuerierHandler hbaseQuerierHandler;

    @PostMapping("/HBase/specify")
    public Object handleSpecifyQuery(@RequestBody SpecificQuery specificQuery){
        // System.out.println(specificQuery.getTime_from().getDay_of_month());
        if(specificQuery.getIdList().size() == 1) { //IdList不为空
            String querierID = specificQuery.getIdList().get(0);
            switch (querierID) { //目前只支持一个querierID的查询
                case "1":{ //by time
                    if(specificQuery.getAnswerType().equals("data")){
                        return hbaseQuerierHandler.queryByDateForData(specificQuery);
                    }else {
                        return hbaseQuerierHandler.queryByDateForTest(specificQuery);
                    }
                }
                case "2":{ //by title
                    if(specificQuery.getAnswerType().equals("data")){
                        return hbaseQuerierHandler.queryByTitleForData(specificQuery);
                    }else {
                        return hbaseQuerierHandler.queryByDirectorForTest(specificQuery);
                    }
                }
                case "3":{ //by director
                    if(specificQuery.getAnswerType().equals("data")){
                        return hbaseQuerierHandler.queryByDirectorForData(specificQuery);
                    }else {
                        return hbaseQuerierHandler.queryByDirectorForTest(specificQuery);
                    }
                }
                case "4":{//by actor
                    if(specificQuery.getAnswerType().equals("data")){
                        return hbaseQuerierHandler.queryByActorForData(specificQuery);
                    }else {
                        return hbaseQuerierHandler.queryByActorForTest(specificQuery);
                    }
                }
                case "6":{
                    if(specificQuery.getAnswerType().equals("data")){
                        return hbaseQuerierHandler.queryByActorForData(specificQuery);
                    }else {
                        return hbaseQuerierHandler.queryByTypeForTest(specificQuery);
                    }
                }
//                case "7":{
//                    if(specificQuery.getAnswerType().equals("data")){
//                        return querier.getDataAnswer(specificQuery);
//                    }else {
//                        return querier.getTestAnswer(specificQuery);
//                    }
//                }
            }
        }
        //IdList为空，返回空查询结果
        return new DataAnswer();
    }
    @PostMapping("/HBase/general/collaboration")
    public Object handleCollaborationQuery(@RequestBody CollaborateQuery collaborateQuery){
        return  new DataAnswer();
    }
}
