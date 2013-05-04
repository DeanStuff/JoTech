package cs.sample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class VehicleRecord2 implements WritableComparable {

    public IntWritable year;
    public Text company;
    public Text model;
    public Text type;

    public void readFields(DataInput in) throws IOException {
        year.readFields(in);
        company.readFields(in);
        model.readFields(in);
        type.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        year.write(out);
        company.write(out);
        model.write(out);
        type.write(out);
    }

    
    public int compareTo(Object obj) {
        VehicleRecord2 vr = (VehicleRecord2) obj;
        int val = this.company.compareTo(vr.company);
        if (val == 0) {
            val = this.model.compareTo(vr.model);
            if (val == 0) {
                val = this.type.compareTo(vr.type);
                if (val == 0) {
                    val = this.year.compareTo(vr.year);
                }
            }
        }
        return val;
    }

    public void loadText(Text rec) {
        String[] pieces = rec.toString().split(SampleDriver.REC_DELINIATOR);

        company.set(pieces[Vehicle.COMPANY.ordinal()]);
        model.set(pieces[Vehicle.MODEL.ordinal()]);
        type.set(pieces[Vehicle.TYPE.ordinal()]);
        year.set(Integer.parseInt(pieces[Vehicle.YEAR.ordinal()].trim()));
    }

    public String getCompany() {
        return company.toString();
    }

    public String getModel() {
        return model.toString();
    }

    public String getType() {
        return type.toString();
    }

    public int getYear() {
        return year.get();
    }


    public String toLog() {
        return "Company: " + company + "  Model: " + model + "  Type: " + type + "  Year: " + year;
    }

}
