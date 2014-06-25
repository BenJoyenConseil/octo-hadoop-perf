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
import wikipedia.options.DateOption;
import wikipedia.reducers.SimpleReducer;

import java.net.URI;

public class SimpleJob extends Configured implements Tool{
    private static final Logger LOG = LoggerFactory.getLogger(SimpleJob.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SimpleJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        conf.setInt("mapreduce.jobtracker.maxtasks.perjob", -1);
        conf.setFloat("mapreduce.job.reduce.slowstart.completedmaps", 0.9f);
        conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);

        Job job = Job.getInstance(conf, "SimpleJob Wikipedia log");
        job.setJarByClass(SimpleJob.class);

        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.addInputPath(job, new Path("/in"));
        CombineTextInputFormat.setInputPathFilter(job, DayDatePathFilter.class);
        CombineTextInputFormat.setMinInputSplitSize(job, 8543441448L / 22L);
        CombineTextInputFormat.setMaxInputSplitSize(job, 8543441448L / 22L);
        WeekDatePathFilter.setDateTime(new DateOption(args).getValue());

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

        return 1;
    }
}
