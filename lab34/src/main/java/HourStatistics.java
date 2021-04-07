import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class HourStatistics extends Configured implements Tool {
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("M/d/y H:m");

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new HourStatistics(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "HourStatistics");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

        job.setMapperClass(HourStatisticsMapper.class);
        job.setReducerClass(HourStatisticsReducer.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(HourStatistics.class);
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

    private static class HourStatisticsMapper extends Mapper<Object, Text, IntWritable, TextArrayWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (datesNotEmpty(fields)) {
                try {
                    LocalDateTime appointmentDate = convertToLocalDateTime(fields[11]);
                    LocalDateTime arrivalDate = convertToLocalDateTime(fields[6]);
                    if (!distanceBetweenDatesExceeds24hours(appointmentDate, arrivalDate)) {
                        int hour = appointmentDate.getHour();
                        TextArrayWritable data = new TextArrayWritable(new LocalDateTime[]{appointmentDate, arrivalDate});
                        context.write(new IntWritable(hour), data);
                    }
                } catch (Exception e) {

                }
            }
        }

        private boolean datesNotEmpty(String[] fields) {
            return !fields[6].equals("") && !fields[11].equals("");
        }

        private boolean distanceBetweenDatesExceeds24hours(LocalDateTime appointmentDate, LocalDateTime arrivalDate) {
            return appointmentDate.isBefore(arrivalDate)
                    ? appointmentDate.plusDays(1).isBefore(arrivalDate)
                    : arrivalDate.plusDays(1).isBefore(appointmentDate);
        }
    }

    private static class HourStatisticsReducer extends Reducer<IntWritable, TextArrayWritable, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            ReduceResult result = new ReduceResult();
            for (TextArrayWritable val : values) {
                Text[] pair = (Text[]) val.toArray();
                LocalDateTime appointmentTime = convertToLocalDateTime(pair[0].toString());
                LocalDateTime arrivalTime = convertToLocalDateTime(pair[1].toString());
                result.includeDates(appointmentTime, arrivalTime);

            }
            context.write(key, new Text(result.toString()));
        }

        private long calculateDifferenceInMinutes(LocalDateTime date1, LocalDateTime date2) {
            Duration duration = Duration.between(date1, date2);
            return duration.toMinutes();
        }

        private class ReduceResult {
            private int numberOfPlannedAppointments = 0;
            private int numberOfDelayedVisitors = 0;
            private int numberOfVisitorsArrivedAheadOfTime = 0;
            private double totalDelay = 0.0;
            private double totalSpeedUp = 0.0;

            public void includeDates(LocalDateTime appointmentTime, LocalDateTime arrivalTime) {
                numberOfPlannedAppointments++;

                long differenceInMinutes = calculateDifferenceInMinutes(appointmentTime, arrivalTime);
                if (differenceInMinutes < 0) {
                    numberOfVisitorsArrivedAheadOfTime++;
                    totalSpeedUp += -differenceInMinutes;
                } else if (differenceInMinutes > 0) {
                    numberOfDelayedVisitors++;
                    totalDelay += -differenceInMinutes;
                }
            }

            @Override
            public String toString() {
                return String.format("%d %d %.1f %d %.1f %.1f",
                        numberOfPlannedAppointments,
                        numberOfDelayedVisitors,
                        getAverageDelay(),
                        numberOfVisitorsArrivedAheadOfTime,
                        getAverageSpeedUp(),
                        getMeanAbsoluteError()
                );
            }

            private double getAverageSpeedUp() {
                return numberOfVisitorsArrivedAheadOfTime == 0 ? 0.0 : totalSpeedUp / numberOfVisitorsArrivedAheadOfTime;
            }

            private double getAverageDelay() {
                return numberOfDelayedVisitors == 0 ? 0.0 : totalDelay / numberOfDelayedVisitors;
            }

            private double getMeanAbsoluteError() {
                return (-totalDelay + totalSpeedUp) / numberOfPlannedAppointments;
            }
        }
    }

    private static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(LocalDateTime[] dates) {
            super(Text.class);
            Text[] datesAsText = new Text[dates.length];
            for (int i = 0; i < dates.length; i++) {
                datesAsText[i] = new Text(dateToString(dates[i]));
            }
            set(datesAsText);
        }

        private String dateToString(LocalDateTime date) {
            return date.format(dateTimeFormatter);
        }
    }

    private static LocalDateTime convertToLocalDateTime(String dateAsString) {
        return LocalDateTime.parse(dateAsString, dateTimeFormatter);
    }
}
