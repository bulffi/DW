package com.f4.DWQueryServer.entity.query;

import lombok.Data;

/**
 * @program: DW
 * @description: To query the users
 * @author: Zijian Zhang
 * @create: 2019/12/04
 **/
@Data
public class ClientQuery {
    private int type;
    private int threshold;
    private double delta;
}
