package pl.edu.pg.eti.karsi.pagerank.reducer;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CalculatePageRankReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalChance = 0.0;
        String[] neighbours = new String[0];
        for (Text value : values) {
            String[] splitted = value.toString().split(" ");
            if (splitted.length == 1) { // only value
                totalChance += Double.parseDouble(value.toString());
            } else {
                neighbours = splitted;
            }
        }
        neighbours[0] = String.valueOf(totalChance);
        String result = "";
        for (String neighbour : neighbours) {
            result = result.concat(neighbour);
            result = result.concat(" ");
        }
        context.write(key, new Text(result));
    }
}