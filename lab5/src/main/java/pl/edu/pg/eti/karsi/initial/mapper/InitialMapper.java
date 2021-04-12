package pl.edu.pg.eti.karsi.initial.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InitialMapper extends Mapper<Object, Text, Text, Text> {

    private int N;

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        this.N = Integer.parseInt(conf.get("N"));
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splitted = line.split(":");
        String startNode = splitted[0];

        double initialPercentage = 1.0 / this.N;
        String nodeData = String.format("%s%s", String.valueOf(initialPercentage), splitted[1]);
        context.write(new Text(startNode), new Text(nodeData));
    }
}

