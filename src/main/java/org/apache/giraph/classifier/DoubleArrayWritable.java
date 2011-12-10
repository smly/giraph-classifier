package org.apache.giraph.classifier;

import java.util.Iterator;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.json.JSONArray;
import org.json.JSONException;

public class DoubleArrayWritable extends ArrayWritable implements Iterable<Double> {
    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }
    
    public String toString() {
        JSONArray jsonVal = new JSONArray();
        try {
            for (String fVal : this.toStrings()) {
                jsonVal.join(fVal);
            }
        } catch (JSONException e) {
            throw new IllegalArgumentException(
                "DoubleArrayWritable.toString: Couldn't convert DoubleArray to JSON", e);
        }
        
        StringBuffer sb = new StringBuffer("[ ");
        String[] aw = this.toStrings();
        for (int i = 0; i < aw.length; ++i) {
            sb.append(aw[i]);
            if (i < aw.length - 1) {
                sb.append(',');
            }
            sb.append(' ');
        }
        sb.append("]");
        return sb.toString();
    }
    
    public String[] elems() {
        return toStrings();
    }
    
    @Override
    public Iterator<Double> iterator() {
        final String[] elems = elems();
        return new Iterator<Double>() {
            private int i = 0;
            
            public boolean hasNext() {
                return i < elems.length;
            }
            
            public Double next() {
                return Double.valueOf(elems[i++]);
            }
            
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}