import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class AirAggPartitioner extends
        Partitioner<Text, Text> implements Configurable {

    private static final String MIN_YEAR = "min.year";
    private Configuration conf = null;
    private int minYear = 0;

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        String[] arr = key.toString().split(",");
        String[] time = arr[0].split("-");
        return Integer.parseInt(time[0].substring(1)) - minYear;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
        minYear = conf.getInt(MIN_YEAR, 2000);
    }

    public static void setMinYear(Job job, int minYear) {
        job.getConfiguration().setInt(MIN_YEAR, minYear);
    }

}
