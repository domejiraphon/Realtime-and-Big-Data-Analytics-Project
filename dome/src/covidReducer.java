import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class covidReducer extends
    Reducer<Text, covidAttributes, NullWritable, Text> {
     
  @Override
  public void reduce(Text key, Iterable<covidAttributes> values, Context context)
    throws IOException, InterruptedException {
      int sumConfirmed = 0;
      int sumRecovered = 0;
      for (covidAttributes val: values){
        sumConfirmed += 
          (val.getnewConfirmed().get() == -1) ? 0 : val.getnewConfirmed().get();
        sumRecovered += 
          (val.getnewRecovered().get() == -1) ? 0 : val.getnewRecovered().get();
      }
      String kk = key.toString();
      Text out = new Text(
          kk + "," + String.valueOf(sumConfirmed) + ","+String.valueOf(sumRecovered));
      
      context.write(NullWritable.get(), out);
  }
}