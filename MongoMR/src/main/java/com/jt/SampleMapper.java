package com.jt;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.bson.BSONObject;

public class SampleMapper extends
        Mapper<Object, BSONObject, Text, VehicleRecord> {

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
    protected void map(Object key, BSONObject value, Context context)
            throws IOException, InterruptedException {
    	
    	log.info("Key is: " + key);
    	log.info("Key type is: " + key.getClass().getName());
    	log.info("Value is: " + value);
    	
    	
    	Text msg = new Text();
//      bBuff = new String();
        vehicleRecord = new VehicleRecord();
//      filterModel = context.getConfiguration().get(SampleDriver.COMPANY_FILTER, "AMC");
        vehicleRecord.setCompany(value.get(VehicleRecord.COMPANY).toString());
        vehicleRecord.setModel(value.get(VehicleRecord.MODEL).toString());
        vehicleRecord.setType(value.get(VehicleRecord.TYPE).toString());
        vehicleRecord.setYear(Integer.parseInt(value.get(VehicleRecord.YEAR).toString()));
        
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
    	log.info("Mapper setup:");
    	vehicleRecord = new VehicleRecord();
    	
    	
    }

}
