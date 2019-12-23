import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class HbaseQuerier {
    public static void main(String[] args){
        HbaseQuerier querier = new HbaseQuerier();
        String[] weekdayMap = {"Mon", "Tue", "Wed","Thi","Fri","Sat","Sun"};
        Scanner scanner = new Scanner(System.in);
        System.out.println("Now begin querying!");
        String order = scanner.nextLine();

        while(!order.equals("")){
            String[] params = order.split("-");
            switch(params[0]){
                case "time":
                    int year = Integer.parseInt(params[1]);
                    int season = Integer.parseInt(params[2]);
                    int month = Integer.parseInt(params[3]);
                    int weekday = Integer.parseInt(params[4]);
                    if(params[5].equals("0")){
                        if(weekday > 0){
                            querier.queryByWeekdayRaw(weekdayMap[weekday]);
                        }else if(season > 0){
                            querier.queryBySeasonRaw(year, season);
                        }else{
                            querier.queryByTimeRaw(year, month);
                        }
                    }else{
                        if(weekday > 0){
                            querier.queryByWeekday(weekdayMap[weekday]);
                        }else if(season > 0){
                            querier.queryBySeason(year, season);
                        }else{
                            querier.queryByTime(year, month);
                        }
                    }
                    break;
                case "actor":
                    if(params[2].equals("0")){
                        querier.queryByActorRaw(params[1]);
                    }else {
                        querier.queryByActor(params[1]);
                    }
                    break;
                case "director":
                    if(params[2].equals("0")){
                        querier.queryByDirectorRaw(params[1]);
                    }else {
                        querier.queryByDirector(params[1]);
                    }
                    break;
                case "type":
                    if(params[2].equals("0")){
                        querier.queryByTypeRaw(params[1]);
                    }else {
                        querier.queryByType(params[1]);
                    }
                    break;
                case "title":
                    querier.queryByTitle(params[1]);
                    break;
                case "user":
                    double score_from = Double.parseDouble(params[2]);
                    double score_to = Double.parseDouble(params[3]);
                    if(params[5].equals("0")) {
                        querier.queryByUserRaw(params[1], score_from, score_to);
                    }else {
                        int numThreads = Integer.parseInt(params[4]);
                        querier.queryByUser(params[1], score_from, score_to, numThreads);
                    }
                default:
                    break;
            }
            order = scanner.nextLine();
        }
    }

    void queryByType(String filmType){
        Connection conn = getLocalHbaseConn();
        try {
            Table table = conn.getTable(TableName.valueOf("dw_movieInfo_type"));
            Filter filter = new PrefixFilter(Bytes.toBytes(filmType));
            Scan scan = new Scan();
            scan.setFilter(filter);

            ResultScanner resultScanner = table.getScanner(scan);
            long start = System.currentTimeMillis();
            int index = 0;
            for(Result result : resultScanner){
                index++;
                System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("typeMap"), Bytes.toBytes("title"))));
            }
            resultScanner.close();
            long cost = System.currentTimeMillis() - start;
            System.out.println("Costs " + cost + ", all " + index);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    void queryByTypeRaw(String filmType){
        Connection conn = getLocalHbaseConn();
        long start = System.currentTimeMillis();
        int index = 0;
        try {
            Table table = conn.getTable(TableName.valueOf("dw_movieInfo"));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("movie"), Bytes.toBytes("type"), CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(filmType)));
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner resultScanner = table.getScanner(scan);


            for(Result result : resultScanner){
                index++;
                System.out.println(Bytes.toString(result.getRow()));
            }
            resultScanner.close();
            long cost = System.currentTimeMillis() - start;
            System.out.println("Costs " + cost + ", all " + index);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void queryByTime(int year, int month){
        long start = System.currentTimeMillis();
        int index = 0;
        try {
            Table talbe = getLocalHbaseConn().getTable(TableName.valueOf("dw_movieInfo_time"));
            String date = "";

            Scan scan = new Scan();
            if(year > 0){
                date += year;
            }
            if(month > 0){
                date += "-";
                date += month < 10 ? ("0" + month) : month;
            }

            Filter filter = new PrefixFilter(Bytes.toBytes(date));
            scan.setFilter(filter);
            scan.withStartRow(Bytes.toBytes(String.valueOf(year)));
            ResultScanner resultScanner = talbe.getScanner(scan);

            for(Result result : resultScanner){
                index++;
                System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("timeMap"), Bytes.toBytes("title")))
                + Bytes.toString(result.getRow()));
            }
            resultScanner.close();

            long cost = System.currentTimeMillis() - start;
            System.out.println("costs " + cost + ", " + index);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    void queryByTimeRaw(int year, int month){
        long start = System.currentTimeMillis();
        int index = 0;
        try {
            String date = "";
            Table table = getLocalHbaseConn().getTable(TableName.valueOf("dw_movieInfo"));

            Scan scan = new Scan();
            if(year > 0){
                date += year;
            }
            if(month > 0){
                date += "-";
                date += month < 10 ? ("0" + month) : month;
            }

            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("movie"), Bytes.toBytes("date"), CompareFilter.CompareOp.EQUAL, new SubstringComparator(date));
            scan.setFilter(filter);
            ResultScanner resultScanner = table.getScanner(scan);

            for(Result result : resultScanner){
                index++;
                System.out.println(Bytes.toString(result.getRow()) + Bytes.toString(result.getValue(Bytes.toBytes("movie"),
                        Bytes.toBytes("date"))));
            }

            resultScanner.close();
            long cost = System.currentTimeMillis() - start;
            System.out.println( "costs " + cost + ", all " + index);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    void queryBySeason(int year, int season){
        long cost = System.currentTimeMillis();
        int index = 0;
        Connection conn = getLocalHbaseConn();
        try {
            Table table = getLocalHbaseConn().getTable(TableName.valueOf("dw_movieInfo_time"));
            String date = "" + year;
            String start, end;
            Scan scan = new Scan();


            if(season == 4){
                start = "" + year + "-10";
                end = "" + (year + 1);
            }else{
                start = "" + year + "-0" + (3 * season - 2);
                end = "" + year + "-0" + (3 * season + 1);
            }
            Filter filter = new PrefixFilter(Bytes.toBytes(date));
            scan.withStartRow(Bytes.toBytes(start));
            scan.withStopRow(Bytes.toBytes(end), false);
            scan.setFilter(filter);

            ResultScanner resultScanner = table.getScanner(scan);
            for(Result result : resultScanner){
                index ++;
                System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("timeMap"), Bytes.toBytes("title")))
                        + Bytes.toString(result.getRow()));
            }

            resultScanner.close();
            cost = System.currentTimeMillis() - cost;
            System.out.println("cost " + cost + ", all " + index);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    void queryBySeasonRaw(int year, int season){
        long cost = System.currentTimeMillis();
        int index = 0;
        Connection conn = getLocalHbaseConn();
        try {
            Table table = getLocalHbaseConn().getTable(TableName.valueOf("dw_movieInfo"));
            String date = "" + year;
            String start, end;

            Scan scan = new Scan();
            if(season == 4){
                start = "" + year + "-10";
                end = "" + (year + 1);
            }else{
                start = "" + year + "-0" + (3 * season - 2);
                end = "" + year + "-0" + (3 * season + 1);
            }

            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            Filter filter1 = new ColumnValueFilter(Bytes.toBytes("movie"), Bytes.toBytes("date"),
                    CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(start)));
            Filter filter2 = new ColumnValueFilter(Bytes.toBytes("movie"), Bytes.toBytes("date"),
                    CompareOperator.LESS, new BinaryComparator(Bytes.toBytes(end)));
            filterList.addFilter(filter1);
            filterList.addFilter(filter2);

            scan.setFilter(filterList);

            ResultScanner resultScanner = table.getScanner(scan);
            for(Result result : resultScanner){
                index ++;
                System.out.println(Bytes.toString(result.getRow()) + Bytes.toString(result.getValue(Bytes.toBytes("movie"),
                        Bytes.toBytes("date"))));
            }

            resultScanner.close();
            cost = System.currentTimeMillis() - cost;
            System.out.println("cost " + cost + ", all " + index);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }


    void queryByWeekday(String weekday){
        long cost = System.currentTimeMillis();
        int index = 0;
        Connection conn = getLocalHbaseConn();
        try {
            Table table = getLocalHbaseConn().getTable(TableName.valueOf("dw_movieInfo_weekday"));

            Scan scan = new Scan();
            scan.setFilter(new PrefixFilter(Bytes.toBytes(weekday)));

            ResultScanner resultScanner = table.getScanner(scan);
            for(Result result : resultScanner){
                index ++;
                result.getValue(Bytes.toBytes("weekdayMap"), Bytes.toBytes("title"));
                //System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("weekdayMap"), Bytes.toBytes("title")))
                        //+ Bytes.toString(result.getRow()));
            }

            resultScanner.close();
            cost = System.currentTimeMillis() - cost;
            System.out.println("cost " + cost + ", all " + index);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    void queryByWeekdayRaw(String weekday){
        long cost = System.currentTimeMillis();
        int index = 0;
        Connection conn = getLocalHbaseConn();
        try {
            Table table = getLocalHbaseConn().getTable(TableName.valueOf("dw_movieInfo"));

            Scan scan = new Scan();

            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("movie"), Bytes.toBytes("date")
                    , CompareFilter.CompareOp.EQUAL, new SubstringComparator(weekday));

            scan.setFilter(filter);

            ResultScanner resultScanner = table.getScanner(scan);
            for(Result result : resultScanner){
                index ++;
                result.getRow();
                //System.out.println(Bytes.toString(result.getRow()) + Bytes.toString(result.getValue(Bytes.toBytes("movie"),
                       // Bytes.toBytes("date"))));
            }

            resultScanner.close();
            cost = System.currentTimeMillis() - cost;
            System.out.println("cost " + cost + ", all " + index);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }





    void queryByTitle(String title){
        long start = System.currentTimeMillis();
        int index = 0;
        Connection conn = getLocalHbaseConn();
        try {

            Scan scan = new Scan();

            BinaryComparator comparator = new BinaryComparator(Bytes.toBytes(title));
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, comparator);
            scan.setFilter(rowFilter);
            Table table = conn.getTable(TableName.valueOf("dw_movieInfo"));
            ResultScanner scanner = table.getScanner(scan);

            for(Result result: scanner){
                index++;
                System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("type"))));
            }

            start = System.currentTimeMillis() - start;
            System.out.println("Costs " + start);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    void queryByActor(String actor){
        Connection conn = getLocalHbaseConn();
        long start = System.currentTimeMillis();
        int index = 0;
        try {
            Table table = conn.getTable(TableName.valueOf("dw_movieInfo_actor"));
            Filter filter = new PrefixFilter(Bytes.toBytes(actor));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(actor));
            scan.setFilter(filter);

            ResultScanner resultScanner = table.getScanner(scan);

            for(Result result : resultScanner){
                index++;
                System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("actorMap"), Bytes.toBytes("title"))));
            }
            resultScanner.close();

            long cost = System.currentTimeMillis() - start;
            System.out.println("Costs " + cost + ", all " + index);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void queryByActorRaw(String actor){
        Connection conn = getLocalHbaseConn();
        long start = System.currentTimeMillis();
        int index = 0;
        try {
            Table table = conn.getTable(TableName.valueOf("dw_movieInfo"));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("movie"), Bytes.toBytes("actor"), CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator(actor));
            Scan scan = new Scan();
            scan.setFilter(filter);

            ResultScanner resultScanner = table.getScanner(scan);

            for(Result result : resultScanner){
                index++;
                System.out.println(Bytes.toString(result.getRow()));
            }
            resultScanner.close();

            long cost = System.currentTimeMillis() - start;
            System.out.println("Costs " + cost + ", all " + index);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void queryByDirector(String director){
        Connection conn = getLocalHbaseConn();
        long start = System.currentTimeMillis();
        int index = 0;
        try {
            Table table = conn.getTable(TableName.valueOf("dw_movieInfo_director"));
            Filter filter = new PrefixFilter(Bytes.toBytes(director));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(director));
            scan.setFilter(filter);

            ResultScanner resultScanner = table.getScanner(scan);

            for(Result result : resultScanner){
                index++;
                System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("directorMap"), Bytes.toBytes("title"))));
            }
            resultScanner.close();

            long cost = System.currentTimeMillis() - start;
            System.out.println("Costs " + cost + ", all " + index);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void queryByDirectorRaw(String director){
        Connection conn = getLocalHbaseConn();
        long start = System.currentTimeMillis();
        int index = 0;
        try {
            Table table = conn.getTable(TableName.valueOf("dw_movieInfo"));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("movie"), Bytes.toBytes("director"), CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator(director));
            Scan scan = new Scan();
            scan.setFilter(filter);

            ResultScanner resultScanner = table.getScanner(scan);

            for(Result result : resultScanner){
                index++;
                System.out.println(Bytes.toString(result.getRow()));
            }
            resultScanner.close();

            long cost = System.currentTimeMillis() - start;
            System.out.println("Costs " + cost + ", all " + index);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void queryByUserRaw(String user, double score_from, double score_to){
        Connection conn = getLocalHbaseConn();
        long start = System.currentTimeMillis();
        int index = 0;
        try {
            Table table = conn.getTable(TableName.valueOf("dw_movieComment"));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("comment"), Bytes.toBytes("profile_name"),
                    CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(user)));
            Scan scan = new Scan();
            scan.setFilter(filter);

            ResultScanner resultScanner = table.getScanner(scan);

            for(Result result : resultScanner){
                byte[] movieScore = result.getValue(Bytes.toBytes("comment"), Bytes.toBytes("score"));
                if(movieScore != null && Bytes.toDouble(movieScore) >= score_from && Bytes.toDouble(movieScore) <= score_to) {
                    index++;
                    System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("comment"),
                            Bytes.toBytes("title"))));
                }
            }

            long cost = System.currentTimeMillis() - start;
            System.out.println("Costs " + cost + ", all " + index);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void queryByUser(String user, double score_from, double score_to, int numThreads){
        long start = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(numThreads);
        List<String>[] results = new ArrayList[numThreads];
        int interval = 7910000 / numThreads + 1;
        int count = 0;

        for(int i = 0; i < results.length; i++){
            results[i] = new ArrayList<>();
        }


        for(int i = 0; i < results.length; i++) {
            new QueryThread(user, score_from, score_to,interval * i, interval * (i + 1), results[i], latch).start();
        }


        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long cost = System.currentTimeMillis() - start;
        for(int i = 0; i < results.length; i++){
            count += results[i].size();
        }
        System.out.println("Costs " + cost + ", all " + count);
    }


    Connection getLocalHbaseConn(){
//        Configuration config = HBaseConfiguration.create();
//        try {
//            config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
//            Connection conn = ConnectionFactory.createConnection(config);
//            return conn;
//        }catch (URISyntaxException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return null;
        return getRemoteHbaseConn();
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

}
