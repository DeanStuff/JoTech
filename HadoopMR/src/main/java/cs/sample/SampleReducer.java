package cs.sample;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class SampleReducer extends
        Reducer<Text, VehicleRecord, Text, NullWritable> {

    static Logger log = Logger.getLogger(SampleReducer.class.getName());

    int cnt = 0;
    int totalCnt = 0;
    
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
    }

    protected void reduce(Text key, Iterable<VehicleRecord> values,
            Context context)
            throws IOException, InterruptedException {
        
        cnt = 0;  // reset counter for each set of values per key
        
        // receiving records ordered by vehicle type
        // so lets treturn all cars found before 1985
        for (VehicleRecord vr : values) {
            log.info("Get vr: " + vr.toLog());
            if (vr.getYear() <= 1985) {
                log.info("Write model: " + vr.getModel());
                context.write(new Text(vr.getModel() + "  " + vr.getYear()), NullWritable.get());
                cnt++;
            }
            totalCnt++;
        }
        
        context.write(new Text("Total qualified records: " + cnt + " out of: " + totalCnt), NullWritable.get());
        

    }

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        cnt = 0;
        totalCnt = 0;
    }

}
