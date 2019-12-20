import entity.Comment;
import entity.Movie;
import org.neo4j.driver.v1.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
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
  public int inCount = 0;
  public int outCount = 0;
  private static final Logger logger = Logger.getLogger(MovieInfoLoader.class.getName());

  public MovieInfoLoader(Driver driver) {
    this.driver = driver;
  }

  private MovieInfoLoader(String uri, String user, String passwd){
    driver = GraphDatabase.driver(uri, AuthTokens.basic(user,passwd));
  }

  private void addCommentInfo(final List<Comment> comments) {
//    match(m:MOVIE_DEMO{id:2}) merge(u:USER_DEMO{id:1}) merge(u)-[:SAY]->(c:COM_DEMO{sum:'sfs',score:3})-[:COM_ON_DEMO]->(m) on create set m.total = m.total + c.score, m.comCount = m.comCount + 1
      try (Session session = driver.session()) {
        try (Transaction transaction = session.beginTransaction()) {
          for(Comment comment:comments){
            if(movieExist(comment.getProduct_id())){
              transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) " +
                      "merge(u:USER{user_id: $user_id, profile_name: $profile_name}) " +
                      "merge(u)-[:SAYS]->(c:COMMENT{helpfulness:$helpfulness, score:$score,review_time:$review_time, summary:$summary, review_text:$text})-[:COMMENT_ON]->(m) " +
                      "on create set m.commentNumber = m.commentNumber + 1, m.totalScore = m.totalScore + $score",
                      parameters("id", comment.getProduct_id(),
                              "user_id", comment.getUser_id(), "profile_name", comment.getProfile_name(),
                              "helpfulness", comment.getHelpfulness(), "score", comment.getScore(),
                              "review_time", comment.getReview_time().getTime(), "summary", comment.getSummary(),
                              "text", comment.getReview_text()));
            inCount ++ ;
            }
            else {
              outCount ++;
            }
          }
          transaction.success();
        }
      }
  }

  private boolean movieExist(String id){
    try(Session session = driver.session()) {
      try(Transaction transaction = session.beginTransaction()) {
        StatementResult result = transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) return m", parameters("id",id));
        transaction.success();
        // System.out.println(id + ": " + (result.list().size() == 1));
        return result.list().size() == 1;
      }
    }
  }

  private void addMovieInfo(final List<Movie> movies) throws ParseException {

    try(Session session = driver.session()) {
      try (Transaction transaction = session.beginTransaction()) {
        for(Movie movie:movies) {
          Date date = movie.getReleaseDate();  //1
          if (date == null) {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd");
            date = format.parse("1900-01-01");
          }
          List<String> IDs = movie.getId();     //1
          String temptID = IDs.get(0);
          List<String> directors = movie.getDirector();
          List<String> actors = movie.getActor();
          String title = movie.getTitle();     //1
          if (title.startsWith("\"")) {
            title = title.substring(1);
          }
          if (title.endsWith("\"")) {
            title = title.substring(0, title.length() - 1);
          }
          List<String> versions = movie.getVersion();
          List<String> types = movie.getType();
          transaction.run("create(id: MOVIE_ID{id:$id}) merge(m: MOVIE{name: $name, releaseDate: $date," +
                          " commentNumber: 0, totalScore: 0}) merge (id)-[:IDENTIFIES]->(m)",
                  parameters("id", temptID, "name", title, "date", date.getTime()));
          for (String director : directors) {
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
          for (String version : versions) {
            transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) merge (v:VERSION{name:$name}) merge (v)<-[:DELIVER_IN]-(m)",
                    parameters("id", temptID, "name", version));
          }
          for (String type : types) {
            transaction.run("match (idNode:MOVIE_ID{id:$id})-[:IDENTIFIES]->(m:MOVIE) merge (t:TYPE{name:$name}) merge (t)<-[:CATEGORY_IN]-(m)",
                    parameters("id", temptID, "name", type));
          }
          for (String id : IDs) {
            transaction.run("match (mainID:MOVIE_ID{id:$temptID})-[:IDENTIFIES]->(m:MOVIE) merge (id:MOVIE_ID{id:$id})" +
                    "merge (id)-[:IDENTIFIES]->(m)", parameters("id", id, "temptID", temptID));
          }
        }
        transaction.success();
      }
    }
  }

  public void close() throws Exception {
    driver.close();
  }

  public static void main(String[] args) throws Exception {
    MovieInfoLoader loader = new MovieInfoLoader("bolt://localhost:7687","zzj","zzjHH");
//    BufferedReader reader = new BufferedReader(new FileReader("Data/part-r-00000(1)"));
//    String line = "";
//    int count = 0;
//    // load the movie info into the database
//    logger.info("Begin to load movie info into neo4j");
//    List<Movie> movieList = new ArrayList<>();
//    do{
//      line = reader.readLine();
//      if(line!=null) {
//        count ++;
//        Movie movie = JSON.parseObject(line, Movie.class);
//        if(!loader.movieExist(movie.getId().get(0))) {
//          movieList.add(movie);
//        }
//        if(movieList.size() % 1000 == 0){
//          loader.addMovieInfo(movieList);
//          movieList.clear();
//        }
//        if (count % 1000 == 0){
//          logger.info("Movie inserted #" + count);
//        }
//      }
//    }while (line!=null);
//    loader.addMovieInfo(movieList);
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
      List<Comment> commentList = new LinkedList<>();
      while (resultSet.next()){
        commentCount++;
        if(commentCount % 10000 == 0){
          logger.info("Comment inserted #"+commentCount);
          logger.info("in: "+loader.inCount+"  out: " + loader.outCount);
        }
        try {
          Comment comment = new Comment();
          comment.setProduct_id(resultSet.getString(1));
          comment.setUser_id(resultSet.getString(2));
          comment.setProfile_name(resultSet.getString(4));
          comment.setHelpfulness(resultSet.getString(5));
          comment.setScore(resultSet.getDouble(6));
          comment.setReview_time(resultSet.getDate(7));
          comment.setSummary(resultSet.getString(8));
          comment.setReview_text(resultSet.getString(9));
          commentList.add(comment);
          if (commentList.size() % 1000 == 0){
            loader.addCommentInfo(commentList);
            commentList.clear();
          }
        }catch (Exception e){
          e.printStackTrace();
        }
      }
      loader.addCommentInfo(commentList);
    }catch (Exception e){
      e.printStackTrace();
    }
    loader.close();
    logger.info("Finished!");
  }
}
