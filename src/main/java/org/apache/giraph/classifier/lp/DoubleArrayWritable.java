package org.apache.giraph.classifier.lp;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayWritable extends ArrayWritable {
    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }
    
    public String toString() {
        StringBuffer sb = new StringBuffer("{ ");
        for (String elem : this.toStrings()) {
            sb.append(elem).append(' ');
        }
        sb.append("}");
        return sb.toString();
    }
}