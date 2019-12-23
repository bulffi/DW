import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class QueryThread extends Thread {
    private String user;
    private double score_from, score_to;
    private int start;
    private int stop;
    private HbaseQuerier querier;
    private CountDownLatch latch;
    private List<String> movies;

    public QueryThread(String user, double score_from, double score_to, int start ,int stop, List<String> movies, CountDownLatch latch){
        this.user = user;
        this.score_from = score_from;
        this.score_to = score_to;
        this.start = start;
        this.stop = stop;
        this.latch = latch;
        this.movies = movies;
        querier = new HbaseQuerier();
    }

    public void run(){
        Connection conn = querier.getLocalHbaseConn();
        try {
            Table table = conn.getTable(TableName.valueOf("dw_movieComment"));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("comment"), Bytes.toBytes("profile_name"),
                    CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(user)));
            Scan scan = new Scan();
            scan.setFilter(filter);
            if(start >= 0){
                scan.withStartRow(Bytes.toBytes(start));
            }
            if(stop >= 0){
                scan.withStopRow(Bytes.toBytes(stop), false);
            }

            ResultScanner resultScanner = table.getScanner(scan);
            int index = 0;

            for (Result result : resultScanner) {
                index++;
                byte[] movieScoreStr = result.getValue(Bytes.toBytes("comment"), Bytes.toBytes("score"));
                if(movieScoreStr == null){
                    continue;
                }
                double movieScore = Bytes.toDouble(movieScoreStr);
                if (movieScore >= score_from && movieScore <= score_to) {
                    String movie = Bytes.toString(result.getValue(Bytes.toBytes("comment"),
                            Bytes.toBytes("title"))) + "  (" + movieScore + ")";
                    movies.add(movie);
                    System.out.println(movie);
                }
            }
            System.out.println("Sublist has"  + index + " results");
            latch.countDown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
