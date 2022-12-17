import java.io.IOException;
import java.io.*; 
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.Format;
import java.util.Locale;
import java.util.TimeZone; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;
public class TweetsMapper
    extends Mapper<LongWritable, Text, Text, DoubleWritable> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {  
try{
    String line = value.toString();

    String[] line_array = line.split(",");

    String temp_tweet_id = line_array[0];

    Long tweet_id=Long.parseLong(temp_tweet_id);
    
    Long shifted_id = tweet_id >> 22; //applying right shift operator to the tweet ID
    
    String suffix="1288834974657";
    
    Long timestamp = shifted_id + Long.parseLong(suffix);
      
    //Date date = new Date(timestamp);
      
    //SimpleDateFormat sdf = new SimpleDateFormat("EEEE,MMMM d yyyy,h:mm,a", Locale.ENGLISH);
      
    //sdf.setTimeZone(TimeZone.getTimeZone("EST"));
    //String formattedDate = sdf.format(date);
      
    //String[] time_array=formattedDate.split(",");
      
    //String output=time_array[1];

    Date date = new Date(timestamp);

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    String output_key = format.format(date);
      
    //System.out.println(output);

    double output_value=Double.parseDouble(line_array[1]);

    context.write(new Text(output_key), new DoubleWritable(output_value));
}
catch(Exception e){}
  }
}
