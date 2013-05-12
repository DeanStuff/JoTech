package com.jt.sample;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class SampleMapper extends
        Mapper<LongWritable, Text, Text, VehicleRecord> {

    static Logger log = Logger.getLogger(SampleMapper.class.getName());

    // note we would have been better off writing an reader which returned a
    // WritableComparable object to move data more efficiently. But for
    // demonstration purposes, this is sufficient.
    String bBuff = null;

    VehicleRecord vehicleRecord = null;
    
    String filterModel = null;

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // close any open connections.
        super.cleanup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // load writablecomparable to use throughout
        vehicleRecord.loadText(value);
        log.info("Vehicle info: " + vehicleRecord.year);

        // lets count the number of each company
        // that is found in the file and only display
        // it in the job tracker stats
        context.getCounter("Company", vehicleRecord.getCompany()).increment(1);

        // If ya want to filter out manufactures or other types of records
        // then do it here so the follow on doesn't have to wait cycles on it
        if (vehicleRecord.getCompany().equalsIgnoreCase(filterModel)) {
            return;
        }
        
        // lets send all the vehicle records to the reducer ordered by
        // their types. Like Cars versus Trucks
        log.info("Writing key: " + vehicleRecord.getType() + "  value: " + vehicleRecord.toLog());
        context.write(new Text(vehicleRecord.getType()), vehicleRecord);
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        bBuff = new String();
        vehicleRecord = new VehicleRecord();
        filterModel = context.getConfiguration().get(SampleDriver.COMPANY_FILTER, "AMC");
    }

}
