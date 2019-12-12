package com.f4.DWQueryServer.mysql.handlers;

/**
 * XXXXXXXXX科技有限公司 版权所有 © Copyright 2018<br>
 *
 * @Description: <br>
 * @Project: hades <br>
 * @CreateDate: Created in 2019/12/11 17:02 <br>
 * @Author: <a href="abc@qq.com">abc</a>
 */
public class AnswerMapper {
    public String mapAnswer(String answer){ //共九种
        if(answer.equals("count"))
            return "count";
        else if(answer.equals("title"))
            return "movie_title";
        else if(answer.equals("actor"))
            return "actor";
        else if(answer.equals("id"))
            return "movie_id";
        else if(answer.equals("director"))
            return "director_name";
        else if(answer.equals("date"))
            return "date";
        else if(answer.equals("type"))
            return "type";
        else if(answer.equals("version"))
            return "version";
        else if(answer.equals("comment"))
            return "comment";
        return null;
    }
}
