import com.alibaba.fastjson.JSON;
import entity.Movie;
import org.neo4j.driver.v1.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * @program: neo4jInject
 * @description: Load movie info into neo4j db
 * @author: Zijian Zhang
 * @create: 2019/11/24
 **/
public class MovieInfoLoader implements AutoCloseable {
  private final Driver driver;

  public MovieInfoLoader(Driver driver) {
    this.driver = driver;
  }

  public MovieInfoLoader(String uri, String user, String passwd){
    driver = GraphDatabase.driver(uri, AuthTokens.basic(user,passwd));
  }
  

  public void addMovieAlongWithDirectorAndActor(final Movie movie) throws ParseException {
    Date date = movie.getReleaseDate();  //1
    if(date==null){
      SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd");
      date = format.parse("1900-01-01");
    }
    List<String> id = movie.getId();     //1
    String temptID = id.get(0);
    List<String> directors = movie.getDirector();
    List<String> actors = movie.getActor();
    String title = movie.getTitle();     //1
    List<String> versions = movie.getVersion();
    List<String> types = movie.getType();
    try(Session session = driver.session()) {
      try (Transaction transaction = session.beginTransaction()) {
        transaction.run("MERGE ( m : Movie{id:$id, name : $name, releaseDate : $date} )",
          parameters("id", temptID, "name", title, "date", date.getTime()));
        transaction.success();
      }
    }
    try(Session session = driver.session()){
     for (String director:directors){
       try(Transaction transaction = session.beginTransaction()) {
         transaction.run("match(m:Movie{id:$id}) merge (d:DIRECTOR{name:$name}) merge (d)-[:DIRECTED]->(m)",
           parameters("id", temptID, "name", director));
         transaction.success();
       }
     }
    }
      for (int i = 0; i < actors.size(); i++) {
        if (i == 0) {
          try (Session session = driver.session()) {
            session.writeTransaction(transaction -> {
              transaction.run("match(m:Movie{id:$id}) merge (a:ACTOR{name:$name}) merge (a)-[:MAIN_ACT_IN]->(m)",
                parameters("id", temptID, "name", actors.get(0)));
              return 1;
            });
          }
        }
          int finalI = i;
          try (Session session = driver.session()) {
            session.writeTransaction(transaction -> {
              transaction.run("match(m:Movie{id:$id}) merge (a:ACTOR{name:$name}) merge (a)-[:ACT_IN]->(m)",
                parameters("id", temptID, "name", actors.get(finalI)));
              return 1;
            });
          }
      }
      for(String version: versions) {
        try (Session session = driver.session()) {
          session.writeTransaction(transaction -> {
            transaction.run("match(m:Movie{id:$id}) merge (v:VERSION{name:$name}) merge (v)<-[:DELIVER_IN]-(m)",
              parameters("id", temptID, "name", version));
            return 1;
          });
        }
      }
      for(String type:types){
        try(Session session = driver.session()) {
          session.writeTransaction(transaction -> {
            transaction.run("match(m:Movie{id:$id}) merge (t:TYPE{name:$name}) merge (t)<-[:CATEGORY_IN]-(m)",
              parameters("id",temptID,"name",type));
            return 1;
          });
        }
      }
  }

  public void close() throws Exception {
    driver.close();
  }

  public static void main(String[] args) throws Exception {
    MovieInfoLoader loader = new MovieInfoLoader("bolt://localhost:7687","neo4j","test");
    BufferedReader reader = new BufferedReader(new FileReader("/Users/zhangzijian/Downloads/DW/part-r-00000(1)"));
    for (int i = 0; i < 100; i++) {
      String line = reader.readLine();
      Movie movie = JSON.parseObject(line, Movie.class);
      loader.addMovieAlongWithDirectorAndActor(movie);
    }

    loader.close();
  }
}
