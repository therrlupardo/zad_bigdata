package pl.edu.pg.eti.karsi.initial.initializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pl.edu.pg.eti.karsi.InitializedPageRank;
import pl.edu.pg.eti.karsi.initial.mapper.InitialMapper;

public class InitialJobInitializer {

    public static void createJob(Path inputPath, Path outputPath, Configuration configuration) throws Exception {
        Job job = Job.getInstance(configuration, "Initial job");

        job.setOutputKeyClass(Object.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(InitialMapper.class);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJarByClass(InitializedPageRank.class);
        job.waitForCompletion(true);
    }
}
