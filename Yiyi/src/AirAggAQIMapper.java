import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class AirAggAQIMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int statCOL = 5;
    private static final int timeCOL = 4;
    private static final int posCOL = 2;
    private static final int posNameCol = 0;
    private static final int ONE = 1;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] strArr = line.split(",");
        try {
            Float.parseFloat(strArr[statCOL]);
            // key: time,location(state code)
            String outKey = String.format("%s,%s", strArr[timeCOL], strArr[posCOL]);
            // value: flag(AQI),location name,AQI,count
            String outVal = String.format("AQI,%s,%s,%d",
                    strArr[posNameCol], strArr[statCOL],ONE);
            context.write(new Text(outKey), new Text(outVal));
        }catch (NumberFormatException e){}
    }
}
