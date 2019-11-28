package entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.util.Date;
import java.util.List;

/**
 * @program: neo4jInject
 * @description: The movie
 * @author: Zijian Zhang
 * @create: 2019/11/24
 **/
@Data
public class Movie {
  private List<String> id;
  private String title;
  private List<String> director;
  @JSONField(format="yyyy-mm-dd")
  private Date releaseDate;
  private List<String> type;
  private List<String> version;
  private List<String> actor;
}
