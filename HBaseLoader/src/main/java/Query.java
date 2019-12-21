import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;

public class Query {
    public static void main(String[] args){
        Query query = new Query();

    }

    void queryByTime(int year){
        long start = System.currentTimeMillis();
        try {
            LongColumnInterpreter columnInterpreter = new LongColumnInterpreter();
            AggregationClient aggregationClient = getAggregationClient();
            String date = "";

            Scan scan = new Scan();
            scan.setCaching(50);
            if(year > 0){
                date += String.valueOf(year);
            }
            //scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("movie"), Bytes.toBytes("date"), CompareFilter.CompareOp.EQUAL,
            //new SubstringComparator(date)));
            scan.setStartRow(Bytes.toBytes("Ron Rubin"));
            //scan.setStopRow(Bytes.toBytes("S"));
            scan.setFilter(new PrefixFilter("Ron Rubin".getBytes()));

            Long amount = aggregationClient.rowCount(TableName.valueOf("dw_movieInfo_actor"), columnInterpreter, scan);
            long cost = System.currentTimeMillis() - start;
            System.out.println(amount + ", costs " + cost);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    void queryByTime(int year, int month){
        Connection conn = getLocalHbaseConn();
        try {
            LongColumnInterpreter columnInterpreter = new LongColumnInterpreter();
            AggregationClient aggregationClient = getAggregationClient();
            String date = "";

            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("type"));
            if(year > 0){
                date += String.valueOf(year);
            }
            if(month > 0){
                date += "-";
                date += month < 10 ? "0" + month : month;
            }
            scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("movie"), Bytes.toBytes("date"), CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator(date)));

            Long amount = aggregationClient.rowCount(TableName.valueOf("dw"), columnInterpreter, scan);
            System.out.println(amount);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    void queryByTitle(String title){
        long start = System.currentTimeMillis();
        Connection conn = getLocalHbaseConn();
        try {
            LongColumnInterpreter columnInterpreter = new LongColumnInterpreter();
            AggregationClient aggregationClient = getAggregationClient();

            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("title"));
            BinaryComparator comparator = new BinaryComparator(Bytes.toBytes(title));
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, comparator);
            scan.setFilter(rowFilter);
            Table table = conn.getTable(TableName.valueOf("dw"));
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> it = scanner.iterator();
            if(it.hasNext()){
                Result result = it.next();
                System.out.println(title + "-----types------" + result.getValue("movie".getBytes(), "type".getBytes()));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    void queryById(String product_id){
        long start = System.currentTimeMillis();
        Connection conn = getLocalHbaseConn();
        try {
            LongColumnInterpreter columnInterpreter = new LongColumnInterpreter();
            AggregationClient aggregationClient = getAggregationClient();

            Scan scan = new Scan();
            scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("comment"), Bytes.toBytes("product_id"), CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes(product_id))));
            long count = aggregationClient.rowCount(TableName.valueOf("dw2"), columnInterpreter, scan);
            long cost = System.currentTimeMillis() - start;
            System.out.println("count:" + count + ", cost " + cost);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
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

}
