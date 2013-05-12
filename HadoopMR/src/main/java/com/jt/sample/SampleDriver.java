package com.jt.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.myorg.WordCount;

public class SampleDriver extends Configured implements Tool {

    public final static String REC_DELINIATOR = ",";
    
    public final static String COMPANY_FILTER = "filter.exclude.company";
    public final static String MODEL_FILTER = "filter.exclude.model";

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job();

        job.setJarByClass(SampleDriver.class);

        job.setJobName("Sample");

        job.setMapperClass(SampleMapper.class);
        job.setReducerClass(SampleReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VehicleRecord.class);
        
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new SampleDriver(), args);
        System.exit(res);
    }

}
