package pl.edu.pg.eti.karsi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import pl.edu.pg.eti.karsi.finaljob.initializer.FinalJobInitializer;
import pl.edu.pg.eti.karsi.initial.initializer.InitialJobInitializer;
import pl.edu.pg.eti.karsi.pagerank.initializer.CalculatePageRankJobInitializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class InitializedPageRank extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new InitializedPageRank(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        InitialJobInitializer.createJob( new Path(args[0]), getTmpPath(0), configuration);

        int K = Integer.parseInt(this.getConf().get("K"));
        for (int i = 0; i < K; i++) {
            CalculatePageRankJobInitializer.createJob(getTmpPath(i), getTmpPath(i + 1), this.getConf());
        }
        return FinalJobInitializer.createFinalJob(
                getTmpPath(Integer.parseInt(this.getConf().get("K"))),
                new Path(args[1]),
                configuration
                ) ? 1 : 0;
    }

    private Path getTmpPath(int iteration) {
        Configuration conf = this.getConf();
        String temporaryPath = conf.get("tmpPath").concat("_").concat(String.valueOf(iteration));
        return new Path(temporaryPath);
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

}
