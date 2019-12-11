package com.f4.DWQueryServer.entity.query;

import lombok.Data;

/**
 * @program: DW
 * @description: To get the query for collaberation
 * @author: Zijian Zhang
 * @create: 2019/12/04
 **/
@Data
public class CollaborateQuery {
    private int type;
    private int threshold;
}
