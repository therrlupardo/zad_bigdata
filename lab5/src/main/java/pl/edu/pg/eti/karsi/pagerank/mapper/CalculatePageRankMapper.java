package pl.edu.pg.eti.karsi.pagerank.mapper;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CalculatePageRankMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, " ");
        double chanceOfBeingInNode = Double.parseDouble(tokenizer.nextToken());
        double numberOfNeighbours = tokenizer.countTokens();
        while (tokenizer.hasMoreTokens()) {
            String node = tokenizer.nextToken();
            double chanceOfGoingToNode = chanceOfBeingInNode / numberOfNeighbours;
            context.write(new Text(node), new Text(String.valueOf(chanceOfGoingToNode)));
        }
        context.write(key, new Text(String.format("-%s", line)));
    }
}