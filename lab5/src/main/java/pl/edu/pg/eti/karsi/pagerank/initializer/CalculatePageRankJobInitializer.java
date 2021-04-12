package pl.edu.pg.eti.karsi.pagerank.initializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pl.edu.pg.eti.karsi.InitializedPageRank;
import org.apache.hadoop.fs.Path;
import pl.edu.pg.eti.karsi.pagerank.mapper.CalculatePageRankMapper;
import pl.edu.pg.eti.karsi.pagerank.reducer.CalculatePageRankReducer;

public class CalculatePageRankJobInitializer {
    public static void createJob(Path inputPath, Path outputPath, Configuration configuration) throws Exception {
        Job job = Job.getInstance(configuration, "Calculate page rank job");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(CalculatePageRankMapper.class);
        job.setReducerClass(CalculatePageRankReducer.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setJarByClass(InitializedPageRank.class);
        job.waitForCompletion(true);
    }
}
