import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class responseReducer extends
    Reducer<Text, responseTuple, Text, MapWritable> {
     
  @Override
  public void reduce(Text key, Iterable<responseTuple> values, Context context)
    throws IOException, InterruptedException {
      ArrayList<Integer> school = new ArrayList<Integer>();   
      ArrayList<Integer> workplace = new ArrayList<Integer>();  
      
      ArrayList<Integer> income = new ArrayList<Integer>();
      ArrayList<Integer> debt = new ArrayList<Integer>();  

      ArrayList<Integer> testing = new ArrayList<Integer>();
      ArrayList<Integer> contact = new ArrayList<Integer>();

      ArrayList<Integer> facial = new ArrayList<Integer>();
      for (responseTuple val: values){
        if (val.getschoolClosing().get() != -1){
          school.add(val.getschoolClosing().get());
        }
        if (val.getworkplaceClosing().get() != -1){
          workplace.add(val.getworkplaceClosing().get());
        }
        if (val.getincomeSupport().get() != -1){
          income.add(val.getincomeSupport().get());
        }
        if (val.getdebtRelief().get() != -1){
          debt.add(val.getdebtRelief().get());
        }
        if (val.gettestingPolicy().get() != -1){
          testing.add(val.gettestingPolicy().get());
        }
        if (val.getcontactTracing().get() != -1){
          contact.add(val.getcontactTracing().get());
        }
        if (val.getfacialCovering().get() != -1){
          facial.add(val.getfacialCovering().get());
        }
      }
    
      MapWritable out = new MapWritable();
      out.put(new Text("school_closing"),
              new FloatWritable(getMedian(school)));
      out.put(new Text("work_closing"),
              new FloatWritable(getMedian(workplace)));
      out.put(new Text("income_support"),
              new FloatWritable(getMedian(income)));
      out.put(new Text("debt_support"),
              new FloatWritable(getMedian(debt)));
      out.put(new Text("testing_policy"),
              new FloatWritable(getMedian(testing)));
      out.put(new Text("contact_policy"),
              new FloatWritable(getMedian(contact)));
      out.put(new Text("facial_covering_policy"),
              new FloatWritable(getMedian(facial)));
      
      context.write(key, out);
  }

  public Float getMedian(ArrayList<Integer> arr){
    Collections.sort(arr);
    float val;
    if (arr.size() == 0){return (float) 0.0;}
    val = (arr.size() % 2 == 0) ? 
      ((float) arr.get(arr.size()/2) + (float) arr.get(arr.size()/2 - 1)) / 2: (float) arr.get(arr.size()/2);
    return val;
   
  }
}