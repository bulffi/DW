package entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
public class Movie {
    private List<String> id;
    private String title;
    private List<String> director;
    @JSONField(format="yyyy-MMid-dd")
    private Date releaseDate;
    private List<String> type;
    private List<String> version;
    private List<String> actor;
}