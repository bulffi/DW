import com.alibaba.fastjson.JSON;
import entity.Comment;
import entity.Movie;
import org.neo4j.driver.v1.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * @program: neo4jInject
 * @description: Load movie info into neo4j db
 * @author: Zijian Zhang
 * @create: 2019/11/24
 **/
public class MovieInfoLoader implements AutoCloseable {
  private final Driver driver;
  private static final Logger logger = Logger.getLogger(MovieInfoLoader.class.getName());

  public MovieInfoLoader(Driver driver) {
    this.driver = driver;
  }

  private MovieInfoLoader(String uri, String user, String passwd){
    driver = GraphDatabase.driver(uri, AuthTokens.basic(user,passwd));
  }

  private void addCommentInfo(final Comment comment) {
    if (movieExist(comment.getProduct_id())) {
      try (Session session = driver.session()) {
        try (Transaction transaction = session.beginTransaction()) {
          transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) " +
                          "set m.commentNumber = m.commentNumber + 1, m.totalScore = m.totalScore + $score " +
                          "match (c:COMMENT{user_id:$user_id, " +
                          "profile_name:$profile_name, helpfulness:$helpfulness, score:$score," +
                          "review_time:$review_time, summary:$summary, review_text:$text})-[:COMMENT_ON]->(m)",
                  parameters("id", comment.getProduct_id(),
                          "user_id", comment.getUser_id(), "profile_name", comment.getProfile_name(),
                          "helpfulness", comment.getHelpfulness(), "score", comment.getScore(),
                          "review_time", comment.getReview_time().getTime(), "summary", comment.getSummary(),
                          "text", comment.getReview_text()));
          transaction.success();
          return;
        }
      }
    }
    logger.warning("No movie: " + comment.getProduct_id());
  }

  private boolean movieExist(String id){
    try(Session session = driver.session()) {
      try(Transaction transaction = session.beginTransaction()) {
        StatementResult result = transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) return m", parameters("id",id));
        transaction.success();
        return result.hasNext();
      }
    }
  }

  private void addMovieInfo(final Movie movie) throws ParseException {
    Date date = movie.getReleaseDate();  //1
    if(date==null){
      SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd");
      date = format.parse("1900-01-01");
    }
    List<String> IDs = movie.getId();     //1
    String temptID = IDs.get(0);
    List<String> directors = movie.getDirector();
    List<String> actors = movie.getActor();
    String title = movie.getTitle();     //1
    List<String> versions = movie.getVersion();
    List<String> types = movie.getType();
    try(Session session = driver.session()) {
      try (Transaction transaction = session.beginTransaction()) {
        transaction.run("create(id: MOVIE_ID{id:$id}) merge(m: MOVIE{name: $name, releaseDate: $date," +
                        " commentNumber: 0, totalScore: 0}) merge (id)-[:IDENTIFIES]->(m)",
          parameters("id", temptID, "name", title, "date", date.getTime()));
        for (String director:directors) {
          transaction.run(" match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) merge (d:DIRECTOR{name:$name}) merge (d)-[:DIRECTED]->(m)",
                  parameters("id", temptID, "name", director));
        }
        for (int i = 0; i < actors.size(); i++) {
          if (i == 0) {
            transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) merge (a:ACTOR{name:$name}) merge (a)-[:MAIN_ACT_IN]->(m)",
                    parameters("id", temptID, "name", actors.get(0)));
          }
          transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) merge (a:ACTOR{name:$name}) merge (a)-[:ACT_IN]->(m)",
                  parameters("id", temptID, "name", actors.get(i)));
        }
        for(String version: versions) {
          transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) merge (v:VERSION{name:$name}) merge (v)<-[:DELIVER_IN]-(m)",
                  parameters("id", temptID, "name", version));
        }
        for(String type:types){
          transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) merge (t:TYPE{name:$name}) merge (t)<-[:CATEGORY_IN]-(m)",
                  parameters("id",temptID,"name",type));
        }
        for(String id:IDs){
          transaction.run("match (mainID:MOVIE_ID{id:$temptID})-[:IDENTIFIES]->(m:MOVIE) merge (id:MOVIE_ID{id:$id})" +
                  "merge (id)-[:IDENTIFIES]->(m)", parameters("id", id, "temptID", temptID));
        }
        transaction.success();
      }
    }
//    try(Session session = driver.session()){
//     for (String director:directors){
//       try(Transaction transaction = session.beginTransaction()) {
//         transaction.run(" match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) merge (d:DIRECTOR{name:$name}) merge (d)-[:DIRECTED]->(m)",
//           parameters("id", temptID, "name", director));
//         transaction.success();
//       }
//     }
//    }
//      for (int i = 0; i < actors.size(); i++) {
//        if (i == 0) {
//          try (Session session = driver.session()) {
//            session.writeTransaction(transaction -> {
//              transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) merge (a:ACTOR{name:$name}) merge (a)-[:MAIN_ACT_IN]->(m)",
//                parameters("id", temptID, "name", actors.get(0)));
//              return 1;
//            });
//          }
//        }
//          int finalI = i;
//          try (Session session = driver.session()) {
//            session.writeTransaction(transaction -> {
//              transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) merge (a:ACTOR{name:$name}) merge (a)-[:ACT_IN]->(m)",
//                parameters("id", temptID, "name", actors.get(finalI)));
//              return 1;
//            });
//          }
//      }
//      for(String version: versions) {
//        try (Session session = driver.session()) {
//          session.writeTransaction(transaction -> {
//            transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) merge (v:VERSION{name:$name}) merge (v)<-[:DELIVER_IN]-(m)",
//              parameters("id", temptID, "name", version));
//            return 1;
//          });
//        }
//      }
//      for(String type:types){
//        try(Session session = driver.session()) {
//          session.writeTransaction(transaction -> {
//            transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) merge (t:TYPE{name:$name}) merge (t)<-[:CATEGORY_IN]-(m)",
//              parameters("id",temptID,"name",type));
//            return 1;
//          });
//        }
//      }
//      for(String id:IDs){
//        try(Session session = driver.session()) {
//          session.writeTransaction(transaction -> {
//            transaction.run("match (mainID:MOVIE_ID{id:$temptID})-[:IDENTIFIES]->(m:MOVIE) merge (id:MOVIE_ID{id:$id})" +
//                    "merge (id)-[:IDENTIFIES]->(m)", parameters("id", id, "temptID", temptID));
//            return 1;
//          });
//        }
//      }
  }

  public void close() throws Exception {
    driver.close();
  }

  public static void main(String[] args) throws Exception {
    MovieInfoLoader loader = new MovieInfoLoader("bolt://localhost:7687","neo4j","QAZxsw741258");
    BufferedReader reader = new BufferedReader(new FileReader("Data/part-r-00000(1)"));
    String line = "";
    int count = 0;
    // load the movie info into the database
    logger.info("Begin to load movie info into neo4j");
    do{
      line = reader.readLine();
      if(!line.equals("")) {
        count ++;
        Movie movie = JSON.parseObject(line, Movie.class);
        loader.addMovieInfo(movie);
        if (count % 500 == 0){
          logger.info("Movie inserted #" + count);
        }
      }
    }while (!line.equals(""));

    // load the comment info into the database
    logger.info("Begin to load movie comment into neo4j");
    String dbURL = "jdbc:sqlserver://localhost:1433;databaseName=ETL_ass_1;user=zzj;password=zzjHH";
    Connection connection = null;
    java.sql.Statement statement = null;
    try {
      connection = DriverManager.getConnection(dbURL);
      statement = connection.createStatement();
      String query = "select * from ETL_ass_1.dbo.movie_review";
      ResultSet resultSet = statement.executeQuery(query);
      int commentCount = 0;
      while (resultSet.next()){
        commentCount++;
        if(commentCount % 10000 == 0){
          logger.info("Comment inserted #"+commentCount);
        }
        Comment comment = new Comment();
        comment.setProduct_id(resultSet.getString(1));
        comment.setUser_id(resultSet.getString(2));
        comment.setProfile_name(resultSet.getString(4));
        comment.setHelpfulness(resultSet.getString(5));
        comment.setScore(resultSet.getDouble(6));
        comment.setReview_time(resultSet.getDate(7));
        comment.setSummary(resultSet.getString(8));
        comment.setReview_text(resultSet.getString(9));
        loader.addCommentInfo(comment);
      }
    }catch (Exception e){
      logger.warning(e.toString());
    }
    loader.close();
    logger.info("Finished!");
  }
}
