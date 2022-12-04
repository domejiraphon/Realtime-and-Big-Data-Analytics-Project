import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import static java.util.Map.entry; 

public class covidMapper extends 
      Mapper<LongWritable, Text, Text, covidAttributes> {
  private
      covidAttributes outTuple = new covidAttributes();
      Map<String, String> hashTable = Map.ofEntries(
          entry("AL", "Alabama"), entry("AK", "Alaska"), entry("AZ", "Arizona"),
          entry("AR", "Arkansas"), entry("AS", "American Samoa"), entry("CA", "California"),
          entry("CO", "Colorado"), entry("CT", "Connecticut"), entry("DE", "Delaware"),
          entry("DC", "District of Columbia"), entry("FL", "Florida"), entry("GA", "Georgia"),
          entry("GU", "Guam"), entry("HI", "Hawaii"), entry("ID", "Idaho"),
          entry("IL", "Illinois"), entry("Indiana", "IN"), entry("IA", "Iowa"),
          entry("KS", "Kansas"), entry("KY", "Kentucky"), entry("LA", "Louisiana"),
          entry("ME", "Maine"), entry("MD", "Maryland"), entry("MA", "Massachusetts"),
          entry("MI", "Michigan"), entry("MN", "Minnesota"), entry("MS", "Mississippi"),
          entry("MO", "Missouri"), entry("NE", "Nebraska"), entry("NH", "New Hampshire"),
          entry("NJ", "New Jersey"), entry("NM", "New Mexico"), entry("NY", "New York"),
          entry("NC", "North Carolina"), entry("ND", "North Dakota"), entry("MP", "Nothern Mariana Islands"),
          entry("OH", "Ohio"), entry("OK", "Oklahoma"), entry("OR", "Oregon"),
          entry("PA", "Pennsylvania"), entry("PR", "Puerto Rico"), entry("RI", "Rhode Island"),
          entry("SC", "South Carolina"), entry("SD", "South Dakota"), entry("TN", "Tennessee"),
          entry("TX", "Texas"), entry("TT", "Trust Territories"), entry("UT", "Utah"),
          entry("VT", "Vermont"), entry("VA", "Virginia"), entry("VI", "Virgin Islands"),
          entry("WA", "Washington"), entry("WV", "West Virginia"), entry("WI", "Wiscosin"),
          entry("WY", "Wyoning")
      );
  @Override
  public void map(LongWritable key, 
                  Text value, 
                  Context context)
    throws IOException, InterruptedException {
      String parsed = value.toString();
      ArrayList<String> arr = new ArrayList<String>();
      int start=-1;
      for (int end=0; end<=parsed.length(); end++){
        if (end == parsed.length() || parsed.charAt(end) == ','){
          arr.add((start + 1 == parsed.length() ||
            start + 1 == end) ? "-1" :
            parsed.substring(start + 1, end));
          start = end;
        }
      }
  
      String sub = arr.get(1).substring(0, 2);
      if (arr.get(1).length() < 3 || !sub.contains("US")){return;}

      if (isNumeric(arr.get(2))){
        String state;
        String[] all = arr.get(1).split("_");
        state = hashTable.get(all[1]);
        
        outTuple.setlocationKey(new Text(arr.get(1)));
        outTuple.setnewConfirmed(
          new IntWritable((arr.get(2).length() > 0) ? Integer.parseInt(arr.get(2)) : -1));
        outTuple.setnewDeceased(
          new IntWritable((arr.get(3).length() > 0) ? Integer.parseInt(arr.get(3)) : -1));
        outTuple.setnewRecovered(
          new IntWritable((arr.get(4).length() > 0) ? Integer.parseInt(arr.get(4)) : -1));
        outTuple.setnewTested(
          new IntWritable((arr.get(5).length() > 0) ? Integer.parseInt(arr.get(5)) : -1));
        outTuple.setcumulativeConfirmed(
          new IntWritable((arr.get(6).length() > 0) ? Integer.parseInt(arr.get(6)) : -1));
        outTuple.setcumulativeDeceased(
          new IntWritable((arr.get(7).length() > 0) ? Integer.parseInt(arr.get(7)) : -1));
        outTuple.setcumulativeRecovered(
          new IntWritable((arr.get(8).length() > 0) ? Integer.parseInt(arr.get(8)) : -1));
        outTuple.setcumulativeTested(
          new IntWritable((arr.get(9).length() > 0) ? Integer.parseInt(arr.get(9)) : -1));
        
        context.write(new Text(arr.get(0) + "," + state), outTuple);
       
      }
  }

  public static boolean isNumeric(String str) { 
    try {  
      Integer.parseInt(str);  
      return true;
    } 
    catch(NumberFormatException e){  
      return false;  
    }  
  }

  public List<String> split(String str, Character delimiter){
    List<String> list = Arrays.asList(new String[10]);
    int start=0;
    for (int end=0; end<str.length(); end++){
      System.out.println(end);
      if (str.charAt(end) == delimiter){
        System.out.println(str.substring(start, end));
       
        list.add(str.substring(start, end));
        start = end;
      }
    }
    System.exit(0);
    for (int i=0; i<list.size(); i++){
      System.out.println(list.get(i));
    }
    System.exit(0);
    return list;
  }
  
}

