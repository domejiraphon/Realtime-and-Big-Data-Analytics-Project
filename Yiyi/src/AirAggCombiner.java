import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirAggCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int[] count = new int[3];   // AQI, TEMP, PRESS
        float[] sum = new float[3];
        String stateName = "";
        for(Text val: values){
            String[] arr = val.toString().split(",");
            if(arr[0].equals("AQI")){
                count[0]++;
                sum[0] += Float.parseFloat(arr[2]);
                if(stateName.length() == 0){
                    stateName = arr[1];
                }
            }else if(arr[0].equals("TEMP")){
                count[1]++;
                sum[1] += Float.parseFloat(arr[1]);
            }else if(arr[0].equals("PRESS")){
                count[2]++;
                sum[2] += Float.parseFloat(arr[1]);
            }
        }
        if(count[0] != 0){
            float meanAQI = sum[0] / count[0];
            String outValue = String.format("AQI,%s,%.4f,%d",
                    stateName, meanAQI, count[0]);
            context.write(key, new Text(outValue));
        }
        if(count[1] != 0){
            float meanTEMP = sum[1] / count[1];
            String outValue = String.format("TEMP,%.4f,%d",
                    meanTEMP, count[1]);
            context.write(key, new Text(outValue));
        }
        if(count[2] != 0){
            float meanPRESS = sum[2] / count[2];
            String outValue = String.format("PRESS,%.4f,%d",
                    meanPRESS, count[2]);
            context.write(key, new Text(outValue));
        }
    }
}
