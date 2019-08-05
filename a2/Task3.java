import java.io.IOException;
import java.util.StringTokenizer;

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

public class Task3 {

  public static class TokenizerMapper 
    extends Mapper<Object, Text, Text, IntWritable>{
    private Text user = new Text();
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);
  
    public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      for (int i = 1; i < tokens.length; i++) {
        user.set(Integer.toString(i));
        if (tokens[i].isEmpty()){
          context.write(user, zero);
        } else{
          context.write(user, one);
        }
      }
    }
  }

  public static class ListReducer 
    extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable total = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                      Context context
                      ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      total.set(sum);
      context.write(key, total);
    }
  }
    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task3");
    job.setJarByClass(Task3.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(ListReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
