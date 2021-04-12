package pl.edu.pg.eti.karsi.finaljob.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import pl.edu.pg.eti.karsi.finaljob.utils.ComparablePair;

import java.io.IOException;
import java.util.TreeSet;

public  class FinalReducer extends Reducer<NullWritable, Text, Text, DoubleWritable> {
    private final TreeSet<ComparablePair<Double, String>> pageRank = new TreeSet<>();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] pair = value.toString().split(" ");
            String page = pair[1];
            double chance = Double.parseDouble(pair[0]);
            pageRank.add(new ComparablePair<>(chance, page));
        }

        for (ComparablePair<Double, String> item : pageRank) {

            context.write(new Text(item.getValue()), new DoubleWritable(item.getKey()));
        }
    }
}