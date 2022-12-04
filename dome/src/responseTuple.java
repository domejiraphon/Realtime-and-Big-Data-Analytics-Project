import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;

public class responseTuple implements Writable {
    private Text locationKey;

    private IntWritable schoolClosing = new IntWritable(-1);
    private IntWritable workplaceClosing = new IntWritable(-1);
  
    private IntWritable stayAtHome = new IntWritable(-1);
    private IntWritable internationalTravelControls = new IntWritable(-1);

    private IntWritable incomeSupport = new IntWritable(-1);
    private IntWritable debtRelief = new IntWritable(-1);
  
    private IntWritable testingPolicy = new IntWritable(-1);
    private IntWritable contactTracing = new IntWritable(-1);
   
    private IntWritable facialCovering = new IntWritable(-1);
    private IntWritable vaccinationPolicy = new IntWritable(-1);
   
    public responseTuple() {

    }
 
    @Override
    public void readFields(DataInput dataInput) throws IOException {
      int length = dataInput.readInt();
      byte[] loc = new byte[length];
      dataInput.readFully(loc);
      locationKey = new Text(new String(loc, "UTF-8"));
   
      schoolClosing = new IntWritable(dataInput.readInt());
      workplaceClosing = new IntWritable(dataInput.readInt());
     
      stayAtHome = new IntWritable(dataInput.readInt());
      internationalTravelControls = new IntWritable(dataInput.readInt());

      incomeSupport = new IntWritable(dataInput.readInt());
      debtRelief = new IntWritable(dataInput.readInt());

      testingPolicy = new IntWritable(dataInput.readInt());
      contactTracing = new IntWritable(dataInput.readInt());

      facialCovering = new IntWritable(dataInput.readInt());
      vaccinationPolicy = new IntWritable(dataInput.readInt());
     
    }
    
    @Override
    public void write(DataOutput dataOutput) throws IOException {
      
      byte[] data = locationKey.toString().getBytes("UTF-8");
      dataOutput.writeInt(data.length);
      dataOutput.write(data);;

      dataOutput.writeInt(schoolClosing.get());
      dataOutput.writeInt(workplaceClosing.get());
     
      dataOutput.writeInt(stayAtHome.get());
      dataOutput.writeInt(internationalTravelControls.get());

      dataOutput.writeInt(incomeSupport.get());
      dataOutput.writeInt(debtRelief.get());
      
      dataOutput.writeInt(testingPolicy.get());
      dataOutput.writeInt(contactTracing.get());
      
      dataOutput.writeInt(facialCovering.get());
      dataOutput.writeInt(vaccinationPolicy.get());
    }

    public Text getlocationKey() {
      return locationKey;
    }
    public void setlocationKey(Text val) {
        this.locationKey = val;
    }



    public IntWritable getschoolClosing() {
      return schoolClosing;
    }
    public void setschoolClosing(IntWritable val) {
        this.schoolClosing = val;
    }
    public IntWritable getworkplaceClosing() {
      return workplaceClosing;
    }
    public void setworkplaceClosing(IntWritable val) {
        this.workplaceClosing = val;
    }


    public IntWritable getstayAtHome() {
      return stayAtHome;
    }
    public void setstayAtHome(IntWritable val) {
        this.stayAtHome = val;
    }
    public IntWritable getinternationalTravelControls() {
      return internationalTravelControls;
    }
    public void setinternationalTravelControls(IntWritable val) {
        this.internationalTravelControls = val;
    }


    public IntWritable getincomeSupport() {
      return incomeSupport;
    }
    public void setincomeSupport(IntWritable val) {
        this.incomeSupport = val;
    }
    public IntWritable getdebtRelief() {
      return debtRelief;
    }
    public void setdebtRelief(IntWritable val) {
        this.debtRelief = val;
    }


    public IntWritable gettestingPolicy() {
      return testingPolicy;
    }
    public void settestingPolicy(IntWritable val) {
        this.testingPolicy = val;
    }
    public IntWritable getcontactTracing() {
      return contactTracing;
    }
    public void setcontactTracing(IntWritable val) {
        this.contactTracing = val;
    }
  


    public IntWritable getfacialCovering() {
      return facialCovering;
    }
    public void setfacialCovering(IntWritable val) {
        this.facialCovering = val;
    }
    public IntWritable getvaccinationPolicy() {
      return vaccinationPolicy;
    }
    public void setvaccinationPolicy(IntWritable val) {
        this.vaccinationPolicy = val;
    }
  

}