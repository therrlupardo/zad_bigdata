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
import java.util.TreeSet;


public class FindMostFrequentCombinations extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FindMostFrequentCombinations(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "FindMostFrequentCombinations");
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(VisitorTupleMap.class);
        job.setReducerClass(VisitorTupleReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(FindMostFrequentCombinations.class);
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

    public static class VisitorTupleMap extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Configuration conf = context.getConfiguration();
            // this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String entityKey = buildKey(value.toString());
            context.write(new Text(entityKey), new IntWritable(1));
        }

        private String buildKey(String line) {
            String[] splitted = line.split(",");
            return String.format("%s %s %s;%s %s", splitted[0], splitted[1],
                    splitted[2], splitted[19], splitted[20]);

        }
    }

    public static class VisitorTupleReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Integer N;
        private TreeSet<ComparablePair<Integer, String>> countVisitorTuples = new TreeSet<ComparablePair<Integer, String>>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            countVisitorTuples.add(new ComparablePair<Integer, String>(sum, key.toString()));
            if (countVisitorTuples.size() > this.N){
                countVisitorTuples.remove(countVisitorTuples.first());
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (ComparablePair<Integer, String> item: countVisitorTuples) {
                context.write(new Text(item.getValue()), new IntWritable(item.getKey()));
            }
        }
    }
}

// >>> Don't Change

