package com.f4.DWQueryServer.hbase;

import com.f4.DWQueryServer.entity.answer.DataAnswer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

@Service
public class HbaseQuerier {
    DataAnswer queryByType(String filmType){
        Connection conn = getLocalHbaseConn();

        List<String> data = new ArrayList<>();
        DataAnswer dataAnswer = new DataAnswer();
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
                data.add(Bytes.toString(result.getValue(Bytes.toBytes("typeMap"), Bytes.toBytes("title"))));
            }
            resultScanner.close();
            long cost = System.currentTimeMillis() - start;

            dataAnswer.setTime(cost);
            dataAnswer.setData(data);
            System.out.println("Costs " + cost + ", all " + index);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataAnswer;
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

    DataAnswer queryByTime(int year, int month){
        long start = System.currentTimeMillis();
        int index = 0;

        List<String> data = new ArrayList<>();
        DataAnswer dataAnswer = new DataAnswer();
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
                data.add(Bytes.toString(result.getValue(Bytes.toBytes("timeMap"), Bytes.toBytes("title")))
                + Bytes.toString(result.getRow()));
            }
            resultScanner.close();

            long cost = System.currentTimeMillis() - start;

            dataAnswer.setData(data);
            dataAnswer.setTime(cost);
            System.out.println("costs " + cost + ", " + index);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return dataAnswer;
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

    DataAnswer queryBySeason(int year, int season){
        long cost = System.currentTimeMillis();
        int index = 0;
        DataAnswer dataAnswer = new DataAnswer();
        List<String> data = new ArrayList<>();

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
                data.add(Bytes.toString(result.getValue(Bytes.toBytes("timeMap"), Bytes.toBytes("title")))
                        + Bytes.toString(result.getRow()));
            }

            resultScanner.close();
            cost = System.currentTimeMillis() - cost;
            dataAnswer.setTime(cost);
            dataAnswer.setData(data);

            System.out.println("cost " + cost + ", all " + index);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return dataAnswer;
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


    DataAnswer queryByWeekday(String weekday){
        long cost = System.currentTimeMillis();
        int index = 0;
        DataAnswer dataAnswer = new DataAnswer();

        List<String> data = new ArrayList<>();
        Connection conn = getLocalHbaseConn();
        try {
            Table table = getLocalHbaseConn().getTable(TableName.valueOf("dw_movieInfo_weekday"));

            Scan scan = new Scan();
            scan.setFilter(new PrefixFilter(Bytes.toBytes(weekday)));

            ResultScanner resultScanner = table.getScanner(scan);
            for(Result result : resultScanner){
                index ++;
                data.add(Bytes.toString(result.getValue(Bytes.toBytes("timeMap"), Bytes.toBytes("title")))
                        + Bytes.toString(result.getRow()));
            }

            resultScanner.close();
            cost = System.currentTimeMillis() - cost;

            System.out.println("cost " + cost + ", all " + index);
            dataAnswer.setData(data);
            dataAnswer.setTime(cost);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return dataAnswer;
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





    DataAnswer queryByTitle(String title){
        long start = System.currentTimeMillis();
        int index = 0;

        List<String> data = new ArrayList<>();
        DataAnswer dataAnswer = new DataAnswer();
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
                data.add(Bytes.toString(result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("date"))));
                data.add(Bytes.toString(result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("type"))));
                data.add(Bytes.toString(result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("version"))));
                data.add(Bytes.toString(result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("director"))));
                data.add(Bytes.toString(result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("actor"))));
            }

            start = System.currentTimeMillis() - start;

            dataAnswer.setData(data);
            dataAnswer.setTime(start);

            System.out.println("Costs " + start);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return dataAnswer;
    }

    DataAnswer queryByActor(String actor){
        Connection conn = getLocalHbaseConn();
        long start = System.currentTimeMillis();
        int index = 0;

        List<String> data = new ArrayList<>();
        DataAnswer dataAnswer = new DataAnswer();
        try {
            Table table = conn.getTable(TableName.valueOf("dw_movieInfo_actor"));
            Filter filter = new PrefixFilter(Bytes.toBytes(actor));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(actor));
            scan.setFilter(filter);

            ResultScanner resultScanner = table.getScanner(scan);

            for(Result result : resultScanner){
                index++;
                data.add(Bytes.toString(result.getValue(Bytes.toBytes("actorMap"), Bytes.toBytes("title"))));
            }
            resultScanner.close();

            long cost = System.currentTimeMillis() - start;

            dataAnswer.setData(data);
            dataAnswer.setTime(cost);
            System.out.println("Costs " + cost + ", all " + index);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataAnswer;
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

    DataAnswer queryByDirector(String director){
        Connection conn = getLocalHbaseConn();
        long start = System.currentTimeMillis();
        int index = 0;

        List<String> data = new ArrayList<>();
        DataAnswer dataAnswer = new DataAnswer();
        try {
            Table table = conn.getTable(TableName.valueOf("dw_movieInfo_director"));
            Filter filter = new PrefixFilter(Bytes.toBytes(director));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(director));
            scan.setFilter(filter);

            ResultScanner resultScanner = table.getScanner(scan);

            for(Result result : resultScanner){
                index++;
                data.add(Bytes.toString(result.getValue(Bytes.toBytes("directorMap"), Bytes.toBytes("title"))));
            }
            resultScanner.close();

            long cost = System.currentTimeMillis() - start;

            dataAnswer.setTime(cost);
            System.out.println("Costs " + cost + ", all " + index);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataAnswer;
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

    void queryByScore(Double score){
        Connection conn = getLocalHbaseConn();
        long start = System.currentTimeMillis();
        int index = 0;
        try {
            Table table = conn.getTable(TableName.valueOf("dw_movieComment"));
            Filter filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(String.valueOf(score))));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(String.valueOf(score)));
            scan.setFilter(filter);

            ResultScanner resultScanner = table.getScanner(scan);

            List<Get> gets = new ArrayList<>();
            for(Result result : resultScanner){
                index++;
                System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("comment"), Bytes.toBytes("title"))));
            }

            long cost = System.currentTimeMillis() - start;
            System.out.println("Costs " + cost + ", all " + index);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
