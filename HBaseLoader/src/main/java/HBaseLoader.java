import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import entity.Comment;
import entity.Movie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HBaseLoader {
    public static void main(String[] args) throws IOException {

        HBaseLoader loader = new HBaseLoader();
        Table table = loader.getLocalHbaseConn().getTable(TableName.valueOf("dw_movieComment"));
        loader.loadId();
        //loader.loadOther();
        //loader.queryByTime(2007);
        //loader.queryByTitle("\"20,000 Leagues Under the Sea\""); ;
        //loader.loadOther();
        //loader.queryById("B003AI2VGA");

        Scan scan = new Scan();

        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("1000"));
        filter = new WhileMatchFilter(filter);
        scan.setFilter(filter);
        long start = System.currentTimeMillis();
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            byte[] shang = result.getValue("comment".getBytes(), "productId".getBytes());
            System.out.println(new String(shang));
        }
        System.out.println(System.currentTimeMillis() - start);

    }

    void loadMovie(){
        FileReader fr = null;
        BufferedReader br = null;
        String line;

        Connection conn = getLocalHbaseConn();

        int count = 0;


        try {
            fr = new FileReader("Data/part-r-00000(1)");
            br = new BufferedReader(fr);

            Table table = conn.getTable(TableName.valueOf("dw"));
            Table time_table = conn.getTable(TableName.valueOf("dw_movieInfo_time"));
            Table director_table = conn.getTable(TableName.valueOf("dw_movieInfo_director"));
            Table actor_table = conn.getTable(TableName.valueOf("dw_movieInfo_actor"));
            Table type_table = conn.getTable(TableName.valueOf("dw_movieInfo_type"));

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-EEEE");

            while((line = br.readLine()) != null) {
                try {
                    Movie movie = JSONObject.parseObject(line, Movie.class);

                    Put put = new Put(Bytes.toBytes(movie.getTitle()));
                    try {
                        put.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("id"), Bytes.toBytes(Joiner.on(",").join(movie.getId())));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has id");
                    }
                    try {
                        put.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("director"), Bytes.toBytes(Joiner.on(",").join(movie.getDirector())));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has director");
                    }
                    try {
                        String date = sdf.format(movie.getReleaseDate());
                        date = date.substring(0, 14);
                        put.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("date"), Bytes.toBytes(date));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has release date");
                        put.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("date"), Bytes.toBytes("nullDate" + count ));
                    }
                    try {
                        put.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("type"), Bytes.toBytes(Joiner.on(",").join(movie.getType())));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has type");
                    }
                    try {
                        put.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("version"), Bytes.toBytes(Joiner.on(",").join(movie.getVersion())));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has version");
                    }
                    try {
                        put.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("actor"), Bytes.toBytes(Joiner.on(",").join(movie.getActor())));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has actor");
                    }

                    table.put(put);

                    if (++count % 100 == 0) {
                        System.out.println("" + count + "row has been inserted");
                    }
                }catch (NullPointerException e){
                    e.printStackTrace();
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                if (fr != null) {
                    fr.close();
                }
                if (br != null) {
                    br.close();
                }
                if(conn != null){
                    conn.close();
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    void loadOthers(){
        FileReader fr = null;
        BufferedReader br = null;
        String line;

        Connection conn = getRemoteHbaseConn();

        int count = 0;


        try {
            fr = new FileReader("Data/part-r-00000(1)");
            br = new BufferedReader(fr);

            int index = 0;

            Table time_table = conn.getTable(TableName.valueOf("dw_movieInfo_time"));
            Table director_table = conn.getTable(TableName.valueOf("dw_movieInfo_director"));
            Table actor_table = conn.getTable(TableName.valueOf("dw_movieInfo_actor"));
            Table type_table = conn.getTable(TableName.valueOf("dw_movieInfo_type"));

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-EEEE");

            List<Put> timePutList = new ArrayList<>();
            List<Put> directorPutList = new ArrayList<>();
            List<Put> actorPutList = new ArrayList<>();
            List<Put> typePutList = new ArrayList<>();


            while((line = br.readLine()) != null) {
                try {
                    Movie movie = JSONObject.parseObject(line, Movie.class);

                    String title = movie.getTitle();
                    String time;
                    try {
                        time = sdf.format(movie.getReleaseDate());
                    }catch (Exception e){
                        time = "0000D" + count;
                    }
                    List<String> directors = movie.getDirector();
                    List<String> actors = movie.getActor();
                    List<String> types = movie.getType();

                    Put put_time = new Put(Bytes.toBytes(time + index++));
                    put_time.addColumn(Bytes.toBytes("timeMap"), Bytes.toBytes("title"), Bytes.toBytes(title));
                    timePutList.add(put_time);

                    for(String director : directors) {
                        Put put_director = new Put(Bytes.toBytes(director + index++));
                        put_director.addColumn(Bytes.toBytes("directorMap"), Bytes.toBytes("title"), Bytes.toBytes(title));
                        directorPutList.add(put_director);
                    }

                    for(String actor : actors){
                        Put put_actor = new Put(Bytes.toBytes(actor + index++));
                        put_actor.addColumn(Bytes.toBytes("actorMap"), Bytes.toBytes("title"), Bytes.toBytes(title));
                        actorPutList.add(put_actor);
                    }

                    for(String type : types){
                        Put put_type = new Put(Bytes.toBytes(type + index++));
                        put_type.addColumn(Bytes.toBytes("typeMap"), Bytes.toBytes("title"), Bytes.toBytes(title));
                        typePutList.add(put_type);
                    }

                    if (++count % 100 == 0) {
                        time_table.put(timePutList);
                        director_table.put(directorPutList);
                        actor_table.put(actorPutList);
                        type_table.put(typePutList);

                        timePutList.clear();
                        directorPutList.clear();
                        actorPutList.clear();
                        typePutList.clear();
                        System.out.println("" + count + "row has been inserted");
                    }
                }catch (NullPointerException e){
                    e.printStackTrace();
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                if (fr != null) {
                    fr.close();
                }
                if (br != null) {
                    br.close();
                }
                if(conn != null){
                    conn.close();
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    void loadId(){
        FileReader fr = null;
        BufferedReader br = null;
        String line;

        Connection conn = getRemoteHbaseConn();

        int count = 0;

        try {
            fr = new FileReader("Data/part-r-00000(1)");
            br = new BufferedReader(fr);


            Table id_table = conn.getTable(TableName.valueOf("dw_movieInfo_id"));

            List<Put> idPutList = new ArrayList<>();

            while((line = br.readLine()) != null) {
                try {
                    Movie movie = JSONObject.parseObject(line, Movie.class);

                    String title = movie.getTitle();

                    List<String> ids = movie.getId();

                    for(String id : ids){
                        Put put_id = new Put(Bytes.toBytes(id));
                        put_id.addColumn(Bytes.toBytes("idMap"), Bytes.toBytes("title"), Bytes.toBytes(title));
                        idPutList.add(put_id);
                    }

                    if (++count % 100 == 0) {
                        id_table.put(idPutList);

                        idPutList = new ArrayList<>();
                        System.out.println("" + count + "row has been inserted");
                    }
                }catch (NullPointerException e){
                    e.printStackTrace();
                }
            }
            id_table.put(idPutList);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                if (fr != null) {
                    fr.close();
                }
                if (br != null) {
                    br.close();
                }
                if(conn != null){
                    conn.close();
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    void loadComment(){
        FileReader fr = null;
        BufferedReader br = null;
        String line;

        Connection conn = getRemoteHbaseConn();

        int count = 0;


        try {
            fr = new FileReader("/home/saturn/Documents/movies.txt");
            br = new BufferedReader(fr);

            Table table = conn.getTable(TableName.valueOf("dw_movieComment"));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Boolean isOver = false;
            List<Put> putList = new ArrayList<>();

            while(!isOver) {
                try {
                    Comment comment = parseComment(br, isOver);
                    Put put;
                    try {
                        double score = comment.getScore();
                        put = new Put(Bytes.toBytes("" + score + count));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has score");
                        continue;
                    }

                    try {
                        put.addColumn(Bytes.toBytes("comment"), Bytes.toBytes("product_id"), Bytes.toBytes(comment.getProduct_id()));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has product id");
                    }
                    try {
                        put.addColumn(Bytes.toBytes("comment"), Bytes.toBytes("user_id"), Bytes.toBytes(comment.getUser_id()));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has user id");
                    }
                    try {
                        put.addColumn(Bytes.toBytes("comment"), Bytes.toBytes("profile_name"), Bytes.toBytes(comment.getProfile_name()));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has release profile name");
                    }
                    try {
                        put.addColumn(Bytes.toBytes("comment"), Bytes.toBytes("helpfulness"), Bytes.toBytes(comment.getHelpfulness()));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has helpfulness");
                    }
//                    try {
//                        put.addColumn(Bytes.toBytes("comment"), Bytes.toBytes("score"), Bytes.toBytes(comment.getScore()));
//                    }catch (NullPointerException e){
//                        System.out.println("Movie" + count + "doesn't has score");
//                    }
                    try {
                        put.addColumn(Bytes.toBytes("comment"), Bytes.toBytes("review_time"), Bytes.toBytes(sdf.format(comment.getReview_time())));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has review time");
                    }
                    try {
                        put.addColumn(Bytes.toBytes("comment"), Bytes.toBytes("summary"), Bytes.toBytes(comment.getSummary()));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has summary");
                    }
                    try {
                        put.addColumn(Bytes.toBytes("comment"), Bytes.toBytes("review_text"), Bytes.toBytes(comment.getReview_text()));
                    }catch (NullPointerException e){
                        System.out.println("Movie" + count + "doesn't has review text");
                    }

                    //putList.add(put);
                    table.put(put);

                    if (++count % 100 == 0) {
                        //table.put(put);
                        //putList = new ArrayList<>();
                        System.out.println("" + count + "row has been inserted");
                    }
                }catch (NullPointerException e){
                    e.printStackTrace();
                }
            }
            table.put(putList);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                if (fr != null) {
                    fr.close();
                }
                if (br != null) {
                    br.close();
                }
                if(conn != null){
                    conn.close();
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    Connection getLocalHbaseConn(){
        Configuration config = HBaseConfiguration.create();
        try {
            config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
            Connection conn = ConnectionFactory.createConnection(config);
            return conn;
        }catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    Connection getRemoteHbaseConn(){
        Configuration config = HBaseConfiguration.create();
        try {
            config.set("hbase.zookeeper.quorum","nn1");  //hbase 服务地址
            config.set("hbase.zookeeper.property.clientPort","2181"); //端口号

            Connection conn = ConnectionFactory.createConnection(config);
            return conn;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    AggregationClient getAggregationClient() {
        Configuration config = HBaseConfiguration.create();
        try{
            config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
            AggregationClient aggregationClient = new AggregationClient(config);
            return aggregationClient;
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

    AggregationClient getRemoteAggregationClient(){
        Configuration config = HBaseConfiguration.create();
        try{
            config.set("hbase.zookeeper.quorum","nn1");  //hbase 服务地址
            config.set("hbase.zookeeper.property.clientPort","2181"); //端口号
            AggregationClient aggregationClient = new AggregationClient(config);
            return aggregationClient;
        }catch (Exception e){

        }
        return null;
    }

    Comment parseComment(BufferedReader br, Boolean isOver){
        int index = 0;
        String lineItem;
        Comment comment = new Comment();
        List<String> lineList = new ArrayList();
        try {
            while((lineItem = br.readLine()) != null && lineItem == ""){

            }
            while(lineItem != null && lineItem.length() != 0){
                lineList.add(lineItem);
                lineItem = br.readLine();
            }
            if(lineItem == null){
                isOver = true;
            }
            for(String line : lineList) {
                int start = line.indexOf(':');
                if(start < 0 || start >= line.length() - 1){
                    continue;
                }
                String key = line.substring(0, start);
                String param = line.substring(start + 1);
                param = param.trim();
                key = key.trim();
                String[] keys = key.split("/");
                if(keys.length != 2){
                    continue;
                }
                key = key.split("/")[1];
                switch (key) {
                    case "productId":
                        comment.setProduct_id(param);
                        break;
                    case "userId":
                        comment.setUser_id(param);
                        break;
                    case "profileName":
                        comment.setProfile_name(param);
                        break;
                    case "helpfulness":
                        comment.setHelpfulness(param);
                        break;
                    case "score":
                        comment.setScore(Double.parseDouble(param));
                        break;
                    case "time":
                        long timestamp = Long.parseLong(param + "000");
                        comment.setReview_time(new Date(timestamp));
                        break;
                    case "summary":
                        comment.setSummary(param);
                        break;
                    case "text":
                        comment.setReview_text(param);
                        break;
                    default:
                        break;
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }catch (NumberFormatException e2){
            e2.printStackTrace();
        }
        return comment;
    }
}
