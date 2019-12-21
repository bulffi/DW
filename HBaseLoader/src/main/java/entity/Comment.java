package entity;
import lombok.Data;

import java.util.Date;

@Data
public class Comment {
    private String product_id;
    private String user_id;
    private String profile_name;
    private String helpfulness;
    private double score;
    private Date review_time;
    private String summary;
    private String review_text;
}

