// package pl.edu.pg.eti.karsi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.TreeSet;


public class InitializedPageRank extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new InitializedPageRank(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        createInitialJob(args);
        int K = Integer.parseInt(this.getConf().get("K"));
        for (int i = 0; i < K; i++) {
            createCalculatePageRankJob(i);
        }
        return createFinalJob(args) ? 1 : 0;
    }

    private Path getTmpPath(int iteration) {
        Configuration conf = this.getConf();
        String temporaryPath = conf.get("tmpPath").concat("_").concat(String.valueOf(iteration));
        return new Path(temporaryPath);
    }

    private void createInitialJob(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Initial job");

        job.setOutputKeyClass(Object.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(InitialMapper.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, getTmpPath(0));

        job.setJarByClass(InitializedPageRank.class);
        job.waitForCompletion(true);
    }

    private void createCalculatePageRankJob(int iteration) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Calculate page rank job");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(CalculatePageRankMapper.class);
        job.setReducerClass(CalculatePageRankReducer.class);

        FileInputFormat.setInputPaths(job, getTmpPath(iteration));
        FileOutputFormat.setOutputPath(job, getTmpPath(iteration + 1));
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setJarByClass(InitializedPageRank.class);
        job.waitForCompletion(true);
    }

    private boolean createFinalJob(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Final job");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(FinalMapper.class);
        job.setReducerClass(FinalReducer.class);
        FileInputFormat.setInputPaths(job, getTmpPath(Integer.parseInt(this.getConf().get("K"))));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setJarByClass(InitializedPageRank.class);
        return job.waitForCompletion(true);
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

    private static class InitialMapper extends Mapper<Object, Text, Text, Text> {

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
//            ArrayList<String> nodeData = new ArrayList<>();
//            nodeData.add(String.valueOf(initialPercentage));
//            nodeData.addAll(Arrays.asList(splitted[1].split(";")));

            context.write(new Text(startNode), new Text(nodeData));
        }
    }

    private static class CalculatePageRankMapper extends Mapper<Text, Text, Text, Text> {

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

    private static class CalculatePageRankReducer extends Reducer<Text, Text, Text, Text> {

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
    
    private static class FinalMapper extends Mapper<Text, Text, NullWritable, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String chance = value.toString().split(" ")[0];
            String result = String.format("%s %s", chance, key.toString());
            context.write(NullWritable.get(), new Text(result));
        }
    }
    
    private static class FinalReducer extends Reducer<NullWritable, Text, Text, DoubleWritable> {
        private TreeSet<ComparablePair<Double, String>> pageRank = new TreeSet<>();
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value: values) {
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

    private static class ComparablePair<A extends Comparable<? super A>,
            B extends Comparable<? super B>>
            extends javafx.util.Pair<A, B>
            implements Comparable<ComparablePair<A, B>> {

        public ComparablePair(A key, B value) {
            super(key, value);
        }

        @Override
        public int compareTo(ComparablePair<A, B> o) {
            int cmp = o == null ? 1 : (this.getKey()).compareTo(o.getKey());
            return cmp == 0 ? (this.getValue()).compareTo(o.getValue()) : cmp;
        }

    }

}
