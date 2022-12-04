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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class trend {
      public static void main(String[] args) throws Exception {
  
  if (args.length != 3) {
    System.err.println("Usage: trend <trend path> <output path> <header>");
    System.exit(-1);
  }
  // second job
  
  Configuration conf = new Configuration();
  conf.set("header", args[2]);
  conf.set("topK", "5");
  conf.setInt("mapreduce.job.running.map.limit", 1);
  Job job2 = Job.getInstance(conf, "google_trends");
  job2.setJarByClass(trend.class);
  job2.setJobName("trend");
  
  FileInputFormat.addInputPath(job2, new Path(args[0]));
  FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_trends"));

  job2.setMapperClass(trendMapper.class);
  
  job2.setMapOutputKeyClass(Text.class);
  job2.setMapOutputValueClass(MapWritable.class);

  job2.setNumReduceTasks(1);
  job2.setReducerClass(trendReducer.class);
  
  job2.setOutputKeyClass(Text.class);
  job2.setOutputValueClass(MapWritable.class);
  job2.waitForCompletion(true);
  }
}