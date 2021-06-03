import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;

// >>> Don't Change
public class TopTitles extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopTitles(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        String temporaryPath = conf.get("tmpPath");
        Path tmpPath = new Path(temporaryPath);
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Title Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(TitleCountMap.class);
        jobA.setReducerClass(TitleCountReduce.class);
        jobA.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopTitles.class);
        boolean result = jobA.waitForCompletion(true);

        // After jobA is finished, all keys are unique regardles the number of reducer that generated them, thus jobB's mappers may keep only the top N pairs.
        // jobA's reducers could limit their outputs by selecting top N local pairs; then the second mapper would be Identity Mapper

        if (result) {
            Job jobB = Job.getInstance(conf, "Top Titles");
            jobB.setOutputKeyClass(Text.class);
            jobB.setOutputValueClass(IntWritable.class);

            jobB.setMapOutputKeyClass(NullWritable.class);
            jobB.setMapOutputValueClass(TextArrayWritable.class);

            jobB.setMapperClass(TopTitlesMap.class);
            jobB.setReducerClass(TopTitlesReduce.class);
            jobB.setNumReduceTasks(1);

            FileInputFormat.setInputPaths(jobB, tmpPath);
            FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

            jobB.setInputFormatClass(KeyValueTextInputFormat.class);
            jobB.setOutputFormatClass(TextOutputFormat.class);

            jobB.setJarByClass(TopTitles.class);
            result = jobB.waitForCompletion(true);
        }
        return result ? 0 : 1;
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

// <<< Don't Change

    public static class TitleCountMap extends Mapper<Object, Text, Text, IntWritable> {
        List<String> stopWords;
        String delimiters;
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            String stopWordsPath = conf.get("stopwords");
            String delimitersPath = conf.get("delimiters");

            this.stopWords = Arrays.asList(readHDFSFile(stopWordsPath, conf).split("\n"));
            this.delimiters = readHDFSFile(delimitersPath, conf);
        }


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenaizer = new StringTokenizer(line, delimiters);
            while (tokenaizer.hasMoreTokens()) {
                String nextToken = tokenaizer.nextToken().trim().toLowerCase();

                if (!stopWords.contains(nextToken)) {
                    context.write(new Text(nextToken), new IntWritable(1));
                }
            }

        }

    }

    public static class TitleCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));

        }
    }

    public static class TopTitlesMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N;
        private TreeSet<ComparablePair<Integer, String>> countToTitleMap = new TreeSet<ComparablePair<Integer, String>>();
        private Text outKey = new Text();
        private IntWritable outValue = new IntWritable();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer count = Integer.parseInt(value.toString());
            String word = key.toString();

            countToTitleMap.add(new ComparablePair<Integer, String>(count, word));
            if (countToTitleMap.size() > this.N) {
                countToTitleMap.remove(countToTitleMap.first());
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (ComparablePair<Integer, String> item : countToTitleMap) {
                String[] strings = {item.getValue(), item.getKey().toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }

        }
    }

    public static class TopTitlesReduce extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {
        Integer N;
        private TreeSet<ComparablePair<Integer, String>> countToTitleMap = new TreeSet<ComparablePair<Integer, String>>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (TextArrayWritable val : values) {
                Text[] pair = (Text[]) val.toArray();
                String word = pair[0].toString();
                Integer count = Integer.parseInt(pair[1].toString());

                countToTitleMap.add(new ComparablePair<Integer, String>(count, word));
                if (countToTitleMap.size() > this.N) {
                    countToTitleMap.remove(countToTitleMap.first());
                }
            }

            for (ComparablePair<Integer, String> item : countToTitleMapcountToTitleMap) {
                Text word = new Text(item.getValue());
                IntWritable value = new IntWritable(item.getKey());
                context.write(word, value);
            }
        }

        // @Override
        // protected void cleanup(Context context) throws IOException, InterruptedException {
        // // TODO

        // }
    }

}

// >>> Don't Change
    class ComparablePair<A extends Comparable<? super A>,
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


class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(String[] strings) {
        super(Text.class);
        Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new Text(strings[i]);
        }
        set(texts);
    }
}
// <<< Don't Change