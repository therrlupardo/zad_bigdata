package pl.edu.pg.eti.karsi.finaljob.initializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pl.edu.pg.eti.karsi.InitializedPageRank;
import pl.edu.pg.eti.karsi.finaljob.mapper.FinalMapper;
import pl.edu.pg.eti.karsi.finaljob.reducer.FinalReducer;

public class FinalJobInitializer {

    public static boolean createFinalJob(Path inputPath, Path outputPath, Configuration configuration) throws Exception {
        Job job = Job.getInstance(configuration, "Final job");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(FinalMapper.class);
        job.setReducerClass(FinalReducer.class);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setJarByClass(InitializedPageRank.class);
        return job.waitForCompletion(true);
    }
}
