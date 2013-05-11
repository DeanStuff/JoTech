package com.jt;

import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class SampleDriver extends Configured implements Tool {
	
	private static final Logger log = Logger.getLogger(SampleDriver.class.getName());
			
    public final static String REC_DELINIATOR = ",";
    
    public final static String COMPANY_FILTER = "filter.exclude.company";
    public final static String MODEL_FILTER = "filter.exclude.model";
    
	Configuration conf = new Configuration();
	
	BasicDBObject searchQuery = null;


    public int run(String[] args) throws Exception {
        Job job = new Job();

        if (conf == null) {
        	conf = new Configuration();
        }
		String db = args[0];
		String collection = args[1];
 
        log.info("Starting to prep mongo connection");
		MongoConfigUtil.setInputURI( conf, "mongodb://localhost/"+db+"."+collection);  // read from testVehicles DB
		// something wrong in the MongoConfigUtil for me.  The INPUT_URI is not being persisted in the
		// config properly.  Setting it here, but a jobConf.xml file may suffice as well.
		job.getConfiguration().set(MongoConfigUtil.INPUT_URI, "mongodb://localhost/"+db+"."+collection);
		
        job.setJarByClass(SampleDriver.class);

        job.setJobName("Sample");

        job.setMapperClass(SampleMapper.class);
        job.setReducerClass(SampleReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VehicleRecord.class);
        
        job.setInputFormatClass(MongoInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setNumReduceTasks(1);

//        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        

    	log.info("Start job launch...");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new SampleDriver(), args);
        System.exit(res);
    }

}
