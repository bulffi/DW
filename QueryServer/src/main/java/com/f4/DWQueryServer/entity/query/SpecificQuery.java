package com.f4.DWQueryServer.entity.query;

import lombok.Data;

import java.util.List;

/**
 * @program: DW
 * @description: To get the sprcific query from front end
 * @author: Zijian Zhang
 * @create: 2019/12/04
 **/
@Data
public class SpecificQuery {
    @Data
    public static class DWTime {
        private int year;
        private int quarter;
        private int month;
        private int day_of_month;
        private int day_of_week;
    }
    private DWTime time_from;
    private DWTime time_to;
    // =========  movie information based condition ============= //
    private String movie_name;
    private String main_actor;
    private List<String> actors;
    private List<String> type;
    private List<String> version;
    private List<String> directors;
    // =========  score & comment based condition ============= //
    @Data
    public static class Comment{
        private String user_id;
        private String user_name;
        private String helpfulness;
        private double score_from;
        private double score_to;
        private DWTime time_from;
        private DWTime time_to;
        private String review_text;
        private String summery;
    }
    private Comment comment;
    // ========= specify which type of query this is to simplify the server side
    private List<String> idList;
    // ========= what do you want?
    // ========= the full answer or just the execution time?
    private String answerType;
    // ========= if you do want answer, which aspect of answer do you want?
    private String answer;
}
