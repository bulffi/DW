import entity.UserScoreMovie;
import org.neo4j.driver.v1.*;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static org.neo4j.driver.v1.Values.parameters;
public class Upgrade2 {
    private final Driver driver;
    private static final Logger logger = Logger.getLogger(Upgrade2.class.getName());
    public Upgrade2(Driver driver) {
        this.driver = driver;
    }

    private Upgrade2(String uri, String user, String passwd){
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user,passwd));
    }

    private void addNewRelation(List<UserScoreMovie> userScoreMovies){
        try(Session session = driver.session()){
            try(Transaction transaction = session.beginTransaction()) {
                for(UserScoreMovie userScoreMovie : userScoreMovies){
                    if(userScoreMovie.getScore() >= 4){
                        transaction.run("match (u:USER{user_id: $id, profile_name: $name}) " +
                                "match (m:MOVIE{name: $movieName, releaseDate: $data, commentNumber: $number}) " +
                                "merge (u)-[:GRADE{point: $point}]->(m)  merge (u)-[:LIKES]-(m)",
                                parameters("id",userScoreMovie.getId(),
                                        "name", userScoreMovie.getName(),
                                        "movieName", userScoreMovie.getMovieName(),
                                        "data", userScoreMovie.getDate(),
                                        "number", userScoreMovie.getNumber(),
                                        "point", userScoreMovie.getScore()));
                    }else {
                        transaction.run("match (u:USER{user_id: $id, profile_name: $name}) " +
                                        "match (m:MOVIE{name: $movieName, releaseDate: $data, commentNumber: $number}) " +
                                        "merge (u)-[:GRADE{point: $point}]->(m)",
                                parameters("id",userScoreMovie.getId(),
                                        "name", userScoreMovie.getName(),
                                        "movieName", userScoreMovie.getMovieName(),
                                        "data", userScoreMovie.getDate(),
                                        "number", userScoreMovie.getNumber(),
                                        "point", userScoreMovie.getScore()));
                    }
                }
                transaction.success();
            }
        }
    }

    private void adjustScore(){
        List<UserScoreMovie> userScoreMovies = new ArrayList<>();
        int count = 0;
        try(Session session = driver.session()){
            StatementResult result = session.run("match(u:USER)-[:SAYS]->(c:COMMENT)-[:COMMENT_ON]->(m:MOVIE) " +
                    "return u.profile_name as name, u.user_id as id, c.score as score, " +
                    "m.name as movieName, m.releaseDate as date, m.commentNumber as number");
            while (result.hasNext()){
                Record record = result.next();
                UserScoreMovie userScoreMovie = new UserScoreMovie();
                String name = record.get("name").asString();
                String id = record.get("id").asString();
                double score = record.get("score").asDouble();
                String movieName = record.get("movieName").asString();
                long data = record.get("date").asLong();
                int number = record.get("number").asInt();
                userScoreMovie.setName(name);
                userScoreMovie.setId(id);
                userScoreMovie.setScore(score);
                userScoreMovie.setMovieName(movieName);
                userScoreMovie.setDate(data);
                userScoreMovie.setNumber(number);
                userScoreMovies.add(userScoreMovie);
                count ++;
                if(count % 1000 == 0){
                    logger.info("Relation inserted #" + count);
                    addNewRelation(userScoreMovies);
                    userScoreMovies.clear();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Upgrade2 loader = new Upgrade2("bolt://localhost:7687", "zzj", "zzjHH");
        loader.adjustScore();
    }
}
