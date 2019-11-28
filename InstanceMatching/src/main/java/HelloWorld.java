/**
 * @program: DW
 * @description: main class for hadoop
 * @author: Zijian Zhang
 * @create: 2019/11/28
 **/
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.alibaba.fastjson.JSON;
import entity.MovieInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import util.StringSetUnionUtil;

public class HelloWorld extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HelloWorld.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

//    job.setOutputKeyClass(NullWritable.class);
//    job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);
        job.setNumReduceTasks(1);
        String[] args = new GenericOptionsParser(getConf(),strings).getRemainingArgs();

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean status = job.waitForCompletion(true);
        return 0;
    }

    public static class WordCountMap extends
            Mapper<LongWritable, Text, Text, Text> {
        private Integer count = 0;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] lines = line.split(",(?=([^\"]|\"[^\"]*\")*$)");

            //System.out.println(lines[1]);
            try {
                String id = lines[0];
                String title = lines[1];
                String director = lines[2];
                String releaseDate = lines[3];
                String type = lines[4];
                String version = lines[5];
                String actor = lines[6];
                MovieInfo info = new MovieInfo();
                info.setId(StringSetUnionUtil.getUnion(new String[]{id}));
                info.setTitle(title);
                info.setDirector(StringSetUnionUtil.getUnion(new String[]{director}));
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
                info.setReleaseDate(dateFormat.parse(releaseDate));
                info.setType(StringSetUnionUtil.getUnion(new String[]{type}));
                info.setVersion(StringSetUnionUtil.getUnion(new String[]{version}));
                info.setActor(StringSetUnionUtil.getUnion(new String[]{actor}));
                List<String> directors = info.getDirector();
                Collections.sort(directors);
                StringBuilder directorKey = new StringBuilder();
                for (String dire :
                        directors) {
                    directorKey.append(dire).append(", ");
                }
                context.write(new Text(lines[1]+" "+directorKey.toString()), new Text(JSON.toJSONString(info)));
            }catch (ArrayIndexOutOfBoundsException | ParseException e){
                System.out.println(value.toString());
            }
        }
    }

    public static class WordCountReduce extends
            Reducer<Text, Text, NullWritable, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            try {
                Text value = new Text();
                Set<String> ids = new HashSet<>();
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
                Date baseDate = simpleDateFormat.parse("1900-1-2");
                Date releaseDate = new Date();
                Set<String> movieType = new HashSet<>();
                Set<String> version = new HashSet<>();
                Set<String> actors = new HashSet<>();
                Set<String> directors = new HashSet<>();
                String title = "";
                for (Text val : values) {
                    String jsonString = val.toString();
                    MovieInfo info = JSON.parseObject(jsonString,MovieInfo.class);
                    ids.addAll(info.getId());
                    movieType.addAll(info.getType());
                    version.addAll(info.getVersion());
                    actors.addAll(info.getActor());
                    directors.addAll(info.getDirector());
                    title = info.getTitle();
                    if(baseDate.before(info.getReleaseDate())){
                        if(releaseDate.after(info.getReleaseDate())){
                            releaseDate = info.getReleaseDate();
                        }
                    }
                }
                if(releaseDate.equals(new Date())){
                    releaseDate = null;
                }
                MovieInfo info=new MovieInfo();
                info.setId(new ArrayList<>(ids));
                info.setActor(new ArrayList<>(actors));
                info.setVersion(new ArrayList<>(version));
                info.setType(new ArrayList<>(movieType));
                info.setDirector(new ArrayList<>(directors));
                info.setReleaseDate(releaseDate);
                info.setTitle(title);
                context.write( NullWritable.get(),new Text(JSON.toJSONString(info)));
            }
            catch(ParseException e){
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        ToolRunner.run(new HelloWorld(),args);
    }
}
