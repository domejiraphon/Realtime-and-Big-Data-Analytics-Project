import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirAggTEMPMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final int statCOL = 16;
    private static final int timeCOL = 11;
    private static final int posCOL = 0;
    private static final int ONE = 1;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] strArr = line.split(",");
        try {
            Float.parseFloat(strArr[statCOL]);
            // key: time,location
            String outKey = String.format("%s,%s", strArr[timeCOL], strArr[posCOL]);
            // value: flag(TEMP),temperature
            String outVal = String.format("TEMP,%s,%d", strArr[statCOL], ONE);
            context.write(new Text(outKey), new Text(outVal));
        }catch (NumberFormatException e){

        }
    }
}
