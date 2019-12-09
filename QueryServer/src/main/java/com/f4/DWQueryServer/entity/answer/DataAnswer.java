package com.f4.DWQueryServer.entity.answer;

import lombok.Data;

import java.util.List;

/**
 * @program: DW
 * @description: The answer of data
 * @author: Zijian Zhang
 * @create: 2019/12/04
 **/
@Data
public class DataAnswer {
    private List<String> data;
    private Long time;
}
