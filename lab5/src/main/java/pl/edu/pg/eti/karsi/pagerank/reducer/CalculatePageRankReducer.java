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
                if (Double.parseDouble(value.toString()) >= 0) {
                    totalChance += Double.parseDouble(value.toString());
                }
            } else {
                neighbours = splitted;
            }
        }
        String result = "";
        for (String item : appendChanceToNeighbours(totalChance, neighbours, context)) {
            result = result.concat(item);
            result = result.concat(" ");
        }
        context.write(key, new Text(result));
    }

    private String[] appendChanceToNeighbours(double totalChance, String[] neighbours, Context context) {
        totalChance = calculateTotalChance(totalChance, context);
        if (neighbours.length == 0) {
            neighbours = new String[1];
        }
        neighbours[0] = String.valueOf(totalChance);
        return neighbours;
    }

    private double calculateTotalChance(double totalChance, Context context) {
        double b = Double.parseDouble(context.getConfiguration().get("B"));
        double n = Double.parseDouble(context.getConfiguration().get("N"));
        return b * totalChance + (1 - b) / n;
    }
}