import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class VisitsDataPerVisitor extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new VisitsDataPerVisitor(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "VisitsDataPerVisitor");
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(VisitorTupleMap.class);
        job.setReducerClass(VisitorTupleReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(VisitsDataPerVisitor.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException {
        Path pt = new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn = new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while ((line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class VisitorTupleMap extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String entityKey = buildKey(line);
            String dateAsString = line.split(",")[11];

            context.write(new Text(entityKey), new Text(dateAsString));
        }

        private String buildKey(String line) {
            String[] splitted = line.split(",");
            return String.format("%s %s %s", splitted[0], splitted[1], splitted[2]);

        }
    }

    public static class VisitorTupleReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Integer totalAppointments = 0;
            LocalDateTime minDate = null;
            LocalDateTime maxDate = null;
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/y H:m");

            for(Text value: values) {
                totalAppointments++;
                LocalDateTime date = LocalDateTime.parse(value.toString(), formatter);
                if (minDate == null || date.isBefore(minDate)) {
                    minDate = date;
                }
                if (maxDate == null || date.isAfter(maxDate)) {
                    maxDate = date;
                }
            }

            String result = String.format("%s %s %d", minDate.format(formatter), maxDate.format(formatter), totalAppointments);
            context.write(key, new Text(result));
        }

    }
}

//// >>> Don't Change
//class ComparablePair<A extends Comparable<? super A>, B extends Comparable<? super B>> extends javafx.util.Pair<A, B>
//        implements Comparable<ComparablePair<A, B>> {
//
//    public ComparablePair(A key, B value) {
//        super(key, value);
//    }
//
//    @Override
//    public int compareTo(ComparablePair<A, B> o) {
//        int cmp = o == null ? 1 : (this.getKey()).compareTo(o.getKey());
//        return cmp == 0 ? (this.getValue()).compareTo(o.getValue()) : cmp;
//    }
//
//}
