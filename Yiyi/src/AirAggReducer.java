import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirAggReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int[] count = new int[3];   // AQI, TEMP, PRESS
        float[] sum = new float[3];
        String stateName = "";
        for(Text val: values){
            String[] arr = val.toString().split(",");
            if(arr[0].equals("AQI")){
                int cnt = Integer.parseInt(arr[3]);
                count[0] += cnt;
                sum[0] += Float.parseFloat(arr[2]) * cnt;
                if(stateName.length() == 0){
                    stateName = arr[1];
                }
            }else if(arr[0].equals("TEMP")){
                int cnt = Integer.parseInt(arr[2]);
                count[1] += cnt;
                sum[1] += Float.parseFloat(arr[1]) * cnt;
            }else if(arr[0].equals("PRESS")){
                int cnt = Integer.parseInt(arr[2]);
                count[2] += cnt;
                sum[2] += Float.parseFloat(arr[1]) * cnt;
            }
        }
        // left join: keep the record only if the AQI exists
        if(stateName.length() > 0){
            // average by state
            float meanAQI = sum[0] / count[0];
            float meanTEMP = count[1] == 0 ? -9999 : sum[1] / count[1];
            float meanPRESS = count[2] == 0 ? -9999 : sum[2] / count[2];
            String outValue = String.format(
                    "%s,%.4f,%.4f,%.4f",
                    stateName, meanAQI, meanTEMP, meanPRESS);
            context.write(key, new Text(outValue));
        }
    }
}
