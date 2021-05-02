import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class YellowTaxiBasic extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new YellowTaxiBasic(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        return createJob(conf, new Path(args[0]), new Path(args[1]));
    }

    private int createJob(Configuration conf, Path inputPath, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "Yellow Taxi Basic - first stage");
        job.setJarByClass(YellowTaxiBasic.class);

        job.setMapperClass(YellowTaxiBasicMapper.class);
        job.setReducerClass(YellowTaxiBasicReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class YellowTaxiBasicMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("y-M-d H:m:s");
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            LocalDateTime dropoffDate = LocalDateTime.parse(fields[2], formatter);
            String newKey = String.format("%s %02d", dropoffDate.getDayOfWeek().toString(), dropoffDate.getHour());
            float tip = Float.parseFloat(fields[13]);
            context.write(new Text(newKey), new FloatWritable(tip));
        }
    }

    private static class YellowTaxiBasicReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float totalTip = 0.0f;
            int numberOfTips = 0;
            for (FloatWritable value : values) {
                numberOfTips ++;
                totalTip += value.get();
            }
            float avgTip = totalTip / numberOfTips;
            context.write(key, new FloatWritable(avgTip));
        }
    }

}
