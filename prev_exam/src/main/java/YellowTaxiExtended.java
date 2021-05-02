import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class YellowTaxiExtended extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new YellowTaxiExtended(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path tmpPath = new Path(conf.get("tmpPath"));
        createInitialJob(conf, new Path(args[0]), tmpPath);
        return createFinalJob(conf, tmpPath, new Path(args[1]));
    }

    private void createInitialJob(Configuration conf, Path inputPath, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "Yellow Taxi Extended - first stage");
        job.setJarByClass(YellowTaxiExtended.class);

        job.setMapperClass(InitialMapper.class);
        job.setReducerClass(InitialReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }

    private int createFinalJob(Configuration conf, Path inputPath, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "Yellow Taxi Extended - second stage");
        job.setJarByClass(YellowTaxiExtended.class);

        job.setMapperClass(FinalMapper.class);
        job.setReducerClass(FinalReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class InitialMapper extends Mapper<Object, Text, Text, FloatWritable> {
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

    private static class InitialReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
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

    private static class FinalMapper extends Mapper<Text, Text, IntWritable, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // key = "{day} {hour}"
            // value = float
            String[] fields = key.toString().split(" ");
            String output = String.format("%s %s", fields[0], value.toString());
            context.write(new IntWritable(Integer.parseInt(fields[1])), new Text(output));
        }
    }

    private static class FinalReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key: hour
            // value: {day} {avgTip}
            String maxDay = "";
            float maxTip = Float.MIN_VALUE;
            for (Text value : values) {
                String[] splitted = value.toString().split(" ");
                float tip = Float.parseFloat(splitted[1]);
                if (tip > maxTip) {
                    maxTip = tip;
                    maxDay = splitted[0];
                }
            }
            String output = String.format("%s\t%f", maxDay, maxTip);
            context.write(key, new Text(output));
        }
    }
}
