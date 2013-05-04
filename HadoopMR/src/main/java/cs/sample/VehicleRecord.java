package cs.sample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class VehicleRecord implements WritableComparable {

    public int year;
    public String company;
    public String model;
    public String type;

    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        company = in.readUTF();
        model = in.readUTF();
        type = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeUTF(company);
        out.writeUTF(model);
        out.writeUTF(type);
    }

    public int compareTo(Object obj) {
        VehicleRecord vr = (VehicleRecord) obj;
        int val = this.company.compareTo(vr.company);
        if (val == 0) {
            val = this.model.compareTo(vr.model);
            if (val == 0) {
                val = this.type.compareTo(vr.type);
                if (val == 0) {
                    val = this.year - vr.year;
                }
            }
        }
        return val;
    }

    public void loadText(Text rec) {
        String[] pieces = rec.toString().split(SampleDriver.REC_DELINIATOR);

        this.company = pieces[Vehicle.COMPANY.ordinal()];
        this.model = pieces[Vehicle.MODEL.ordinal()];
        this.type = pieces[Vehicle.TYPE.ordinal()];
        this.year = Integer.parseInt(pieces[Vehicle.YEAR.ordinal()].trim());

    }

    public String getCompany() {
        return company;
    }

    public String getModel() {
        return model;
    }

    public String getType() {
        return type;
    }

    public int getYear() {
        return year;
    }


    public String toLog() {
        return "Company: " + company + "  Model: " + model + "  Type: " + type + "  Year: " + year;
    }

}
