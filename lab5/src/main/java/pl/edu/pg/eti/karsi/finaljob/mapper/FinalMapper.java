package pl.edu.pg.eti.karsi.finaljob.mapper;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FinalMapper extends Mapper<Text, Text, NullWritable, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String chance = value.toString().split(" ")[0];
        String result = String.format("%s %s", chance, key.toString());
        context.write(NullWritable.get(), new Text(result));
    }
}