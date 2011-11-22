package org.apache.giraph.classifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.giraph.classifier.DoubleArrayWritable;

public final class MulticlassClassifierWritable implements Writable {
    private BooleanWritable isLabeledData;
    /* from 0 to C to represent class label */
    private IntWritable classLabelIndex;
    private DoubleArrayWritable fValues;
    
    public MulticlassClassifierWritable() {
        this.isLabeledData = new BooleanWritable();
        this.classLabelIndex = new IntWritable();
        this.fValues = new DoubleArrayWritable();
    }
    
    public MulticlassClassifierWritable(
            BooleanWritable isLabeled, IntWritable classLabelIndex,
            DoubleArrayWritable fValues) {
        this.isLabeledData = isLabeled;
        this.classLabelIndex = classLabelIndex;
        this.fValues = fValues;
    }
    
    public void set(
            BooleanWritable isLabeled, IntWritable classLabelIndex,
            DoubleArrayWritable fValues) {
        this.isLabeledData = isLabeled;
        this.classLabelIndex = classLabelIndex;
        this.fValues = fValues;
    }
    
    public Integer argMax() {
        Integer argmaxClassLabelIndex = new Integer(-1);
        double val = -1;
        DoubleWritable[] values = (DoubleWritable[]) fValues.get();
        for (int i = 0; i < values.length; ++i) {
            if (val < values[i].get()) {
                argmaxClassLabelIndex = i;
                val = values[i].get();
            }
        }
        
        return argmaxClassLabelIndex;
    }
    
    public String toString() {
        StringBuffer sb = new StringBuffer("{ ");
        /* Labeled: L, Unlabled: U*/
        sb.append("label: " + (isLabeledData.get() ? "L" : "U")).append(", ");
        sb.append("argmax: " + this.argMax().toString()).append(", ");
        sb.append("fval: " + fValues.toString()).append(" }");
        return sb.toString();
    }
    
    public Integer getClassLabelIndex() {
        return classLabelIndex.get();
    }
    
    public void setCurrentValue(Integer index, Double val) {
        Writable values[] = this.fValues.get();
        values[index] = new DoubleWritable(val);
        this.fValues.set(values);
    }
    
    public Double getCurrentValue(Integer index) {
        Double value = new Double(0.0);
        try {
            DoubleWritable[] values = (DoubleWritable[]) fValues.get();
            value = values[index.intValue()].get();
        } catch(Exception e) {
            throw new IllegalArgumentException(
                "MulticlassClassifierWritable: Couldn't get current value.");
        }
        return value;
    }
    
    public DoubleArrayWritable getCurrentValues() {
        return fValues;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        isLabeledData.readFields(in);
        classLabelIndex.readFields(in);
        fValues.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        isLabeledData.write(out);
        classLabelIndex.write(out);
        fValues.write(out);
    }
}
