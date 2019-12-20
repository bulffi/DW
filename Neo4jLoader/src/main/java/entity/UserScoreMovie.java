package entity;

import lombok.Data;

@Data
public class UserScoreMovie {
    // user
    private String name;
    private String id;
    // score
    private double score;
    // movie
    private String movieName;
    private long  date;
    private int number;
}
