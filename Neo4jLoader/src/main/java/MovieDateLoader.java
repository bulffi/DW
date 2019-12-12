import entity.MovieInNeo4j;
import org.neo4j.driver.v1.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;
import static org.neo4j.driver.v1.Values.parameters;
public class MovieDateLoader {
    private final Driver driver;
    public int inCount = 0;
    public int outCount = 0;
    private List<MovieInNeo4j> movieInNeo4js = new ArrayList<>();
    private static final Logger logger = Logger.getLogger(MovieInfoLoader.class.getName());


    public MovieDateLoader(Driver driver) {
        this.driver = driver;
    }
    private MovieDateLoader(String uri, String user, String passwd){
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user,passwd));
    }
    private void handleDate(){
        int count  = 0;
        try(Session session = driver.session()){
            StatementResult statementResult = session.run("match(m:MOVIE) return m.name as name, " +
                    "m.releaseDate as date, m.commentNumber as number, m.totalScore as total");
            while (statementResult.hasNext()){
                count ++;
                if(count % 3000 == 0){
                    logger.info("read # " + count);
                }
                Record record = statementResult.next();
                String name = record.get("name").asString();
                long date = record.get("date").asLong();
                int number = record.get("number").asInt();
                double total = record.get("total").asDouble();
                MovieInNeo4j movieInNeo4j = new MovieInNeo4j();
                movieInNeo4j.setName(name);
                movieInNeo4j.setCommentNumber(number);
                movieInNeo4j.setTotalScore(total);
                movieInNeo4j.setReleaseDate(new Date(date));
                movieInNeo4j.setDateLong(date);
                // logger.info(movieInNeo4j.toString());
                movieInNeo4js.add(movieInNeo4j);
            }
        }
        int count_2 = 0;
        for(MovieInNeo4j movie : movieInNeo4js){
            count_2 ++ ;
            if(count_2 % 1000 == 0){
                logger.info("write # " + count_2);
            }
            Date date = movie.getReleaseDate();
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            int year = calendar.get(Calendar.YEAR);
            SimpleDateFormat format  = new SimpleDateFormat("mm");
            String monthString = format.format(date);
            int month = Integer.parseInt(monthString);
            //logger.info("month " + month);
            int quarter = 0;
            if(month < 4){
                quarter = 1;
            }else if(month <7){
                quarter = 2;
            }else if(month < 10){
                quarter = 3;
            }else {
                quarter = 4;
            }
            int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
            int dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
            // logger.info(date +" " + year +" " + month +" " + quarter + " " + dayOfMonth + " " + dayOfWeek);
            String deal = "match (movie:MOVIE{name: $name, releaseDate: $releaseDate, commentNumber: $number, totalScore: $total}) " +
                    "merge (y:YEAR{num: $year}) " +
                    "merge (y)-[:OF]->(q:QUARTER{num : $quarter}) merge (y)-[:OF]->(movie) " +
                    "merge (q)-[:OF]->(m:MONTH{num : $month}) merge (q)-[:OF]->(movie) " +
                    "merge (m)-[:OF]->(dow:DAY_OF_WEEK{num : $dow}) merge(m)-[:OF]->(movie) " +
                    "merge (dow)-[:OF]->(dom:DAY_OF_MONTH{num: $dom}) merge(dow)-[:OF]->(movie) " +
                    "merge (dom)-[:OF]->(movie)";
            try (Session session = driver.session()){
                session.run(deal,parameters("year",year,"name",movie.getName(),"releaseDate",movie.getDateLong(),
                        "number",movie.getCommentNumber(),"total",movie.getTotalScore(),"quarter",quarter,"month",month,"dow",dayOfWeek,"dom",dayOfMonth));
            }
        }
        driver.close();
    }
    public static void main(String[] args){
        MovieDateLoader loader = new MovieDateLoader("bolt://localhost:7687","zzj","zzjHH");
        loader.handleDate();
    }
}
