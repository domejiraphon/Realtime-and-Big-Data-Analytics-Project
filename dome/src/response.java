import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import java.nio.file.Paths;

public class response {
      public static void main(String[] args) throws Exception {
  
  if (args.length != 2) {
    System.err.println("Usage: response <response path> <output path>");
    System.exit(-1);
  }
  // second job
  Job job3 = Job.getInstance();
  job3.setJarByClass(response.class);
  job3.setJobName("response");

  FileInputFormat.addInputPath(job3, new Path(args[0]));
  FileOutputFormat.setOutputPath(job3, new Path(args[1] + "_response"));

  job3.setMapperClass(responseMapper.class);
  
  job3.setMapOutputKeyClass(Text.class);
  job3.setMapOutputValueClass(responseTuple.class);

  job3.setNumReduceTasks(1);
  job3.setReducerClass(responseReducer.class);
  
  job3.setOutputKeyClass(Text.class);
  job3.setOutputValueClass(MapWritable.class);
  job3.waitForCompletion(true);
  }
}