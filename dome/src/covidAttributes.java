import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;

public class covidAttributes implements Writable {
    private Text locationKey;
    private IntWritable newConfirmed = new IntWritable(-1);
    private IntWritable newDeceased = new IntWritable(-1);
    private IntWritable newRecovered = new IntWritable(-1);
    private IntWritable newTested = new IntWritable(-1);
    private IntWritable cumulativeConfirmed = new IntWritable(-1);
    private IntWritable cumulativeDeceased = new IntWritable(-1);
    private IntWritable cumulativeRecovered = new IntWritable(-1);
    private IntWritable cumulativeTested = new IntWritable(-1);
  
    public covidAttributes() {
    }
    /*
     * @Override
    public void readFields(DataInput dataInput) throws IOException {
      System.out.println("+++++++++++");
      locationKey.readFields(dataInput); 
      newConfirmed.readFields(dataInput); 
      newDeceased.readFields(dataInput);
      newRecovered.readFields(dataInput);
      newTested.readFields(dataInput);
      cumulativeConfirmed.readFields(dataInput);
      cumulativeDeceased.readFields(dataInput);
      cumulativeRecovered.readFields(dataInput);
      cumulativeTested.readFields(dataInput);
    }
    
    @Override
    public void write(DataOutput dataOutput) throws IOException {
      locationKey.write(dataOutput); 
      newConfirmed.write(dataOutput); 
      newDeceased.write(dataOutput);
      newRecovered.write(dataOutput);
      newTested.write(dataOutput);
      cumulativeConfirmed.write(dataOutput);
      cumulativeDeceased.write(dataOutput);
      cumulativeRecovered.write(dataOutput);
      cumulativeTested.write(dataOutput);
      
    }
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
      int length = dataInput.readInt();
      byte[] loc = new byte[length];
      dataInput.readFully(loc);
      locationKey = new Text(new String(loc, "UTF-8"));

      newConfirmed = new IntWritable(dataInput.readInt());
      newDeceased = new IntWritable(dataInput.readInt());
      
      newRecovered = new IntWritable(dataInput.readInt());
      newTested = new IntWritable(dataInput.readInt());
      cumulativeConfirmed = new IntWritable(dataInput.readInt());
      cumulativeDeceased = new IntWritable(dataInput.readInt());
      cumulativeRecovered = new IntWritable(dataInput.readInt());
      cumulativeTested = new IntWritable(dataInput.readInt());
    }
    
    @Override
    public void write(DataOutput dataOutput) throws IOException {
      
      byte[] data = locationKey.toString().getBytes("UTF-8");
      dataOutput.writeInt(data.length);
      dataOutput.write(data);;

      dataOutput.writeInt(newConfirmed.get());
      dataOutput.writeInt(newDeceased.get());

      dataOutput.writeInt(newRecovered.get());
      dataOutput.writeInt(newTested.get());
      dataOutput.writeInt(cumulativeConfirmed.get());
      dataOutput.writeInt(cumulativeDeceased.get());
      dataOutput.writeInt(cumulativeRecovered.get());
      dataOutput.writeInt(cumulativeTested.get());
    }

    public Text getlocationKey() {
      return locationKey;
    }
    public void setlocationKey(Text val) {
        this.locationKey = val;
    }

    public IntWritable getnewConfirmed() {
      return newConfirmed;
    }
    public void setnewConfirmed(IntWritable val) {
        this.newConfirmed = val;
    }

    public IntWritable getnewDeceased() {
      return newDeceased;
    }
    public void setnewDeceased(IntWritable val) {
        this.newDeceased = val;
    }

    public IntWritable getnewRecovered() {
      return newRecovered;
    }
    public void setnewRecovered(IntWritable val) {
        this.newRecovered = val;
    }

    public IntWritable getnewTested() {
      return newTested;
    }
    public void setnewTested(IntWritable val) {
        this.newTested = val;
    }

    public IntWritable getcumulativeConfirmed() {
      return cumulativeConfirmed;
    }
    public void setcumulativeConfirmed(IntWritable val) {
        this.cumulativeConfirmed = val;
    }

    public IntWritable getcumulativeDeceased() {
      return cumulativeDeceased;
    }
    public void setcumulativeDeceased(IntWritable val) {
        this.cumulativeDeceased = val;
    }

    public IntWritable getcumulativeRecovered() {
      return cumulativeRecovered;
    }
    public void setcumulativeRecovered(IntWritable val) {
        this.cumulativeRecovered = val;
    }

    public IntWritable getcumulativeTested() {
      return cumulativeTested;
    }
    public void setcumulativeTested(IntWritable val) {
        this.cumulativeTested = val;
    }
}