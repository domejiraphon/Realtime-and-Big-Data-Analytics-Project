import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Tweets {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: Tweets <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = Job.getInstance();
    job.setJarByClass(Tweets.class);
    job.setJobName("Tweets");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(TweetsMapper.class);
    //job.setCombinerClass(MaxTemperatureReducer.class);
    job.setNumReduceTasks(1);
    job.setReducerClass(TweetsReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}