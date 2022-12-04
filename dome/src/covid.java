import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import java.nio.file.Paths;

public class covid {
      public static void main(String[] args) throws Exception {
  
  if (args.length != 2) {
    System.err.println("Usage: covid <covid path> <output path>");
    System.exit(-1);
  }
  
  Job job = Job.getInstance();
  job.setJarByClass(covid.class);
  job.setJobName("covid");

  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));

  job.setMapperClass(covidMapper.class);
  job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(covidAttributes.class);
 
  job.setNumReduceTasks(1);
  job.setReducerClass(covidReducer.class);
  
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  job.waitForCompletion(true);
  }
}