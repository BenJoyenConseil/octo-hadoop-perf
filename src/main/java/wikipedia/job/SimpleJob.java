package wikipedia.job;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wikipedia.domain.PageNameAndCount;
import wikipedia.filters.date.DayDatePathFilter;
import wikipedia.filters.date.WeekDatePathFilter;
import wikipedia.mappers.SimpleMapper;
import wikipedia.reducers.SimpleReducer;

import java.net.URI;

public class SimpleJob extends Configured implements Tool{
    private static final Logger LOG = LoggerFactory.getLogger(SimpleJob.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SimpleJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        Duration[] durations = new Duration[1];
        args = new String[2];
        args[0] = "-date";

        for(int i = 0; i < durations.length; i++){
           LOG.info("Job n° " + (i+1));

            args[1] = "2014051" + i;
            DateTime start = DateTime.now();

            Configuration conf = getConf();

            conf.setInt("mapreduce.jobtracker.maxtasks.perjob", -1);
            conf.setFloat("mapreduce.job.reduce.slowstart.completedmaps", 0.9f);
            conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);

            Job job = Job.getInstance(conf, "SimpleJob Wikipedia log");
            job.setJarByClass(SimpleJob.class);

            job.setInputFormatClass(CombineTextInputFormat.class);
            CombineTextInputFormat.addInputPath(job, new Path("/in"));
            CombineTextInputFormat.setInputPathFilter(job, DayDatePathFilter.class);
            WeekDatePathFilter.setDateTime(args[1]);

            //8543441448
            CombineTextInputFormat.setMinInputSplitSize(job, 1396019875622L / 25L);
            CombineTextInputFormat.setMaxInputSplitSize(job, 1396019875622L / 25L);

            job.setOutputFormatClass(TextOutputFormat.class);
            Path out = new Path("/out");
            out.getFileSystem(getConf()).delete(out, true);
            TextOutputFormat.setOutputPath(job, out);


            job.setMapperClass(SimpleMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(PageNameAndCount.class);

            job.setCombinerClass(SimpleReducer.class);
            job.setReducerClass(SimpleReducer.class);
            job.setNumReduceTasks(4);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            job.addCacheFile(new URI("conf/page_names_to_skip.txt"));
            job.waitForCompletion(true);
            DateTime end = DateTime.now();
            Duration elapsedTime = new Duration(start, end);
            durations[i] = elapsedTime;
        }

        Path file = new Path("/out/durations.txt");
        FSDataOutputStream out = file.getFileSystem(getConf()).create(file, true);

        int average = 0;
        for(Duration d : durations){
            out.writeUTF(d.getStandardSeconds() + " s " + "\n");
            average += d.getStandardSeconds();
        }
        average /= durations.length;
        out.writeUTF("Moyenne : " + average + " s ");
        out.flush();
        out.close();

        return 1;
    }
}
