import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class trendReducer extends
    Reducer<Text, MapWritable, Text, MapWritable> {
  private int topK;
  @Override
  public void reduce(Text key, Iterable<MapWritable> values, Context context)
    throws IOException, InterruptedException {
      TreeMap<Integer, String> topSym = new TreeMap<Integer, String>();
      HashMap<String, Integer> cache = new HashMap<String, Integer>();
     
      for (MapWritable val: values){
        for (Map.Entry<Writable, Writable> entry : val.entrySet()){
          if (cache.containsKey(entry.getKey().toString())){
            int prev = cache.get(entry.getKey().toString());
            cache.put(entry.getKey().toString(), prev + 1);
          }
          else {
            cache.put(entry.getKey().toString(), 1);
          }
        }
      }
      
      for (Map.Entry<String, Integer> entry : cache.entrySet()){
        
        topSym.put(entry.getValue(), entry.getKey());
        if (topSym.size() > topK){
          topSym.remove(topSym.firstKey());
        }
      }
      MapWritable out = new MapWritable();
        for (Map.Entry<Integer, String> entry : topSym.entrySet()){
          out.put(new Text(entry.getValue()),
                  new IntWritable(entry.getKey()));
        }
      context.write(key, out);
      
  }

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    topK = Integer.parseInt(context.getConfiguration().get("topK"));
  }

}