import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.Comparator;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task1 {

  public static class TokenizerMapper 
    extends Mapper<Object, Text, Text, Text>{
    private Text movie = new Text();
  
    public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      movie.set(tokens[0]);
      List<Integer> ratings = Arrays.asList(Arrays.copyOfRange(tokens, 1, tokens.length)).stream().map(x -> {
        try{
          return Integer.parseInt(x);
        } catch(Exception e){
          return 0;
        }
      }).collect(Collectors.toList());
      int maxRating = Collections.max(ratings);
      for (int i = 0; i < ratings.size(); i++){
        if (ratings.get(i) == maxRating){
          context.write(movie, new Text(Integer.toString(i+1)));
        }
      }
    }
  }

  public static class ListReducer 
    extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, 
                      Context context
                      ) throws IOException, InterruptedException {
      List<Integer> list = new ArrayList<Integer>();
      for (Text t : values){
        list.add(Integer.parseInt(t.toString()));
      }
      Collections.sort(list);
      List<String> result = list.stream().map(i -> String.valueOf(i)).collect(Collectors.toList());
      String output = String.join(",", result);
      context.write(key, new Text(output));
    }
  }
    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task1");
    job.setJarByClass(Task1.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(ListReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
