import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirAggregate {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: AirAggregate <AQI input> <TEMP input> <PRESS input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf);
        job.setJarByClass(AirAggregate.class);
        job.setJobName("AirAggregate");

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, AirAggAQIMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, AirAggTEMPMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]),
                TextInputFormat.class, AirAggPRESSMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        job.setCombinerClass(AirAggCombiner.class);
        job.setPartitionerClass(AirAggPartitioner.class);
        AirAggPartitioner.setMinYear(job, 2019);
        job.setReducerClass(AirAggReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(4);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
