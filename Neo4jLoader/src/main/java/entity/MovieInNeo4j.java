package entity;

import lombok.Data;

import java.util.Date;

@Data
public class MovieInNeo4j {
    private String name;
    private double totalScore;
    private int commentNumber;
    private Date releaseDate;
    private long dateLong;
}
