import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.HashMap;
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
    private HashMap<String, int[]> reference = new HashMap<String, int[]>();
  
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try{
          Path localPath = DistributedCache.getLocalCacheFiles(context.getConfiguration())[0];
          BufferedReader br = new BufferedReader(new FileReader(localPath.toString()));
          String line;
          while ((line = br.readLine()) != null) {
            String[] tokens = line.split(",");
            int[] ratings = new int[tokens.length-1];
            for (int i = 1; i < tokens.length; i++){
              if (tokens[i].equals("")){
                ratings[i-1] = 0;
              } else {
                ratings[i-1] = Integer.parseInt(tokens[i]);
              }
            }
            reference.put(tokens[0], ratings);
          }
        } catch(IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }

    public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      String movie = tokens[0];
      for(String refMovie : reference.keySet()){
        int[] refTokens = reference.get(refMovie);
        int refLen = refTokens.length;
        int similarity = 0;
        if (movie.compareTo(refMovie) < 0){
          for (int i = 1; i < tokens.length; i++) {
            if (!tokens[i].equals("") && Integer.parseInt(tokens[i]) == refTokens[i-1]){
              similarity++;
            }
          }
          context.write(new Text(movie + "," + refMovie), new IntWritable(similarity));
        }
      }
    }
  }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    conf.set("mapreduce.input.fileinputformat.split.maxsize", "8388608");
    conf.set("mapreduce.input.fileinputformat.split.minsize", "2097152");
    
    Job job = Job.getInstance(conf, "Task4");
    job.setJarByClass(Task4.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    job.setMapperClass(TokenizerMapper.class);
    
    DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
