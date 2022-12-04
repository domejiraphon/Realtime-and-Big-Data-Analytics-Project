import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class AirAggregateLocal {
    // col: year, pos, <posName>, stat
    public static TreeMap<String, String> readFile(String in, int[] col) throws IOException{
        Scanner sc = new Scanner(new File(in));
        sc.nextLine();   // skip first row
        Map<String, float[]> map = new HashMap<>();  // year, pos, posName - count, sum

        while(sc.hasNextLine()){
            String[] arr = sc.nextLine().split(",");
            String outKey;
            if(col.length == 4){
                outKey = String.format("%s,%s,%s", arr[col[0]], arr[col[1]], arr[col[2]]);
            }else{
                outKey = String.format("%s,%s", arr[col[0]], arr[col[1]]);
            }
            float[] farr = map.getOrDefault(outKey, new float[2]);
            farr[0]++;
            farr[1] += Float.parseFloat(arr[col[col.length - 1]]);
            map.put(outKey, farr);
        }

        TreeMap<String, String> sortedMap = new TreeMap<>();
        for(Map.Entry<String, float[]> entry: map.entrySet()){
            float[] farr = entry.getValue();
            float mean = farr[1] / farr[0];
            String[] keyArr = entry.getKey().split(",");
            if(keyArr.length == 3){
                String outKey = String.format("%s,%s", keyArr[0], keyArr[1]);
                String outVal = String.format("%s,%.4f", keyArr[2], mean);
                sortedMap.put(outKey, outVal);
            }else{
                sortedMap.put(entry.getKey(), String.format("%.4f", mean));
            }
        }

        return sortedMap;
    }

    public static TreeMap<String, String> aggregate(TreeMap<String, String>[] maps){
        TreeMap<String, String> aggMap = new TreeMap<>();
        final String NAN = String.format("%.4f", -9999.0f);

        for(Map.Entry<String, String> entry: maps[0].entrySet()){
            String temp = maps[1].getOrDefault(entry.getKey(), NAN);
            String press = maps[2].getOrDefault(entry.getKey(), NAN);
            String aggVal = String.format("%s,%s,%s", entry.getValue(), temp, press);
            aggMap.put(entry.getKey(), aggVal);
        }

        return aggMap;
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {
        String inAQI = "/Users/tongyiyi/Documents/RBDA/Project/data/AQI/daily_aqi_by_county_2021.csv";
        String inTEMP = "/Users/tongyiyi/Documents/RBDA/Project/data/TEMP/daily_TEMP_2021.csv";
        String inPRESS = "/Users/tongyiyi/Documents/RBDA/Project/data/PRESS/daily_PRESS_2021.csv";
        String out = "/Users/tongyiyi/Documents/RBDA/Project/output/agg_2021_local.txt";

        // read input files
        TreeMap<String, String>[] maps = new TreeMap[3];
        maps[0] = readFile(inAQI, new int[]{4, 2, 0, 5});
        maps[1] = readFile(inTEMP, new int[]{11, 0, 16});
        maps[2] = readFile(inPRESS, new int[]{11, 0, 16});

        // aggregate
        TreeMap<String, String> aggMap = aggregate(maps);

        // output
        FileWriter fw = new FileWriter(out);
        for(Map.Entry<String, String> entry: aggMap.entrySet()){
            fw.write(String.format("%s,%s\n", entry.getKey(), entry.getValue()));
        }
        fw.close();
    }
}

