package entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.util.Date;
import java.util.List;

/**
 * @program: hadoop
 * @description: The entity of movie info
 * @author: Zijian Zhang
 * @create: 2019/11/23
 **/
@Data
public class MovieInfo {
  private List<String> id;
  private String title;
  private List<String> director;
  @JSONField(format="yyyy-mm-dd")
  private Date releaseDate;
  private List<String> type;
  private List<String> version;
  private List<String> actor;
}
