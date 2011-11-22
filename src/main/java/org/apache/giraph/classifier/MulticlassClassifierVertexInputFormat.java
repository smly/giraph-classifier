/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.classifier;

import org.json.JSONArray;
import org.json.JSONException;
import org.apache.giraph.lib.*;
import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * InputFormat for reading graphs stored as (ordered) adjacency lists
 * with the vertex ids longs and the vertex values and edges doubles.
 * For example:
 * 22 0.1 45 0.3 99 0.44
 * to repesent a vertex with id 22, value of 0.1 and edges to nodes 45 and 99,
 * with values of 0.3 and 0.44, respectively.
 */
public class MulticlassClassifierVertexInputFormat<M extends Writable> extends
    TextVertexInputFormat<LongWritable, MulticlassClassifierWritable, DoubleWritable, M>  {

    static class VertexReader<M extends Writable> extends
        AdjacencyListVertexReader<LongWritable, MulticlassClassifierWritable, DoubleWritable, M> {

    VertexReader(RecordReader<LongWritable, Text> lineRecordReader) {
        super(lineRecordReader);
    }

    VertexReader(RecordReader<LongWritable, Text> lineRecordReader,
                 LineSanitizer sanitizer) {
        super(lineRecordReader, sanitizer);
    }

    @Override
    public void decodeId(String s, LongWritable id) {
        id.set(Long.valueOf(s));
    }

    @Override
    public void decodeValue(String s, MulticlassClassifierWritable value) {
        DoubleArrayWritable daw   = new DoubleArrayWritable();
        BooleanWritable isLabeled = new BooleanWritable(false);
        
        Double argmaxFval = 0.0;
        Integer argmax    = 0;
        try {
            JSONArray jsonFvals = new JSONArray(s);
            
            DoubleWritable dws[] = new DoubleWritable[jsonFvals.length()];
            for (int i = 0; i < jsonFvals.length(); ++i) {
                Double fval = jsonFvals.getDouble(i);   
                if (fval > argmaxFval) {
                    argmaxFval = fval;
                    argmax     = i;
                    isLabeled  = new BooleanWritable(true);
                }
                dws[i] = new DoubleWritable(fval);
            }
            daw.set(dws);
        } catch (JSONException e) {
            throw new IllegalArgumentException(
                "DoubleArrayWritable.toString: Couldn't decode label information", e);
        }
        value.set(isLabeled, new IntWritable(argmax), daw);
    }

    @Override
    public void decodeEdge(String s1, String s2, Edge<LongWritable, DoubleWritable>
        textIntWritableEdge) {
      textIntWritableEdge.setDestVertexId(new LongWritable(Long.valueOf(s1)));
      textIntWritableEdge.setEdgeValue(new DoubleWritable(Double.valueOf(s2)));
    }
  }

  @Override
  public org.apache.giraph.graph.VertexReader<LongWritable,
      MulticlassClassifierWritable, DoubleWritable, M> createVertexReader(
      InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new VertexReader<M>(textInputFormat.createRecordReader(
      split, context));
  }
}
