import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {

  public static class TokenizerMapper 
    extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
  
    public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
      Path localPath = context.getLocalCacheFiles()[0];
      String[] tokens = value.toString().split(",");
      String movie = tokens[0];
      try (BufferedReader br = new BufferedReader(new FileReader(localPath.toString()))) {
        String line;
        while ((line = br.readLine()) != null) {
          String[] lineTokens = line.split(",");
          int lineLen = lineTokens.length;
          String currMovie = lineTokens[0];
          for (int i = 1; i < tokens.length; i++) {
            if (movie.compareTo(currMovie) < 0 && i < lineLen && !tokens[i].equals("") && tokens[i].equals(lineTokens[i])){
              context.write(new Text(movie + "," + currMovie), one);
            }
          }
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
    
    Job job = Job.getInstance(conf, "Task4");
    job.setJarByClass(Task4.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(ListReducer.class);
    
    DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
