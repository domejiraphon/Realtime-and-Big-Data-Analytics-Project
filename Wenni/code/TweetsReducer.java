import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

public class TweetsReducer
    extends Reducer<Text, DoubleWritable, Text, DoubleWritable> { 
  @Override
  public void reduce(Text key,Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {

        double count=0;

        double sum=0;

        for (DoubleWritable value : values){

          count++;

          sum+=value.get();
        }

        double average=sum/count;

        context.write(key, new DoubleWritable(average));
  }
}



