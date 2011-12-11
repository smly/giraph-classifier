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

package org.apache.giraph.classifier.lp;

import org.apache.giraph.classifier.DoubleArrayWritable;
import org.apache.giraph.classifier.MulticlassClassifierWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import java.util.Iterator;

public class GFHF extends
        Vertex<LongWritable, MulticlassClassifierWritable, DoubleWritable, DoubleArrayWritable> {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(GFHF.class);
    public static String MAX_SUPERSTEP = "GFHF.maxSuperstep";
    
    private long getMaxSuperstep() {
        return getContext().getConfiguration().getLong(MAX_SUPERSTEP, 10);
    }
    
    private void initialLabeling() {
        MulticlassClassifierWritable vertexValue = getVertexValue();
        /* initial labeling */
        Integer argMaxIndex = vertexValue.argMax();
        if (vertexValue.isLabeled()) {
            for (int i = 0; i < vertexValue.getClassLabelIndexSize(); ++i) {
                vertexValue.setCurrentValue(i, (argMaxIndex == i)? 1.0 : 0.0);
            }
        }
    }
    
    @Override
    public void compute(Iterator<DoubleArrayWritable> msgIterator) {
        MulticlassClassifierWritable vertexValue = getVertexValue();
        
        Long maxIteration     = getMaxSuperstep();
        Long currentIteration = getSuperstep();
        /* TODO: iterate until convergence to labels (MulticlassClassifierWritable.fValue) */
        if (currentIteration / 3 == maxIteration) {
            voteToHalt();
            return;
        }
        
        switch (currentIteration.intValue() % 3) {
        case 0:
            initialLabeling();
            break;
        case 1:
            Integer argMaxIndex = vertexValue.argMax();
            if (vertexValue.getCurrentValue(argMaxIndex) > 0.0) {
                for (LongWritable targetVertexId : this) {
                    final int elemSize = vertexValue.getClassLabelIndexSize();
                    DoubleWritable fvals[] = (DoubleWritable[]) vertexValue.getCurrentValues().toArray();
                    DoubleWritable edgeWeight = getEdgeValue(targetVertexId);
                    DoubleWritable msg[] = new DoubleWritable[elemSize];
                    for (int i = 0; i < elemSize; ++i) {
                        msg[i] = new DoubleWritable(fvals[i].get() * edgeWeight.get());
                    }
                    DoubleArrayWritable daw = new DoubleArrayWritable();
                    daw.set(msg);
                    sendMsg(targetVertexId, daw);
                }
            }
            break;
        case 2:
            Double norm = 0.0;
            for (LongWritable targetVertexId : this) {
                DoubleWritable edgeValue = this.getEdgeValue(targetVertexId);
                norm += edgeValue.get();
            }
            /* initialize fValue */
            vertexValue.initializeFValue();
            Double tempVals[] = new Double[vertexValue.getClassLabelIndexSize()];
            for (int i = 0; i < vertexValue.getClassLabelIndexSize(); ++i) {
                tempVals[i] = 0.0;
            }
            /* receive messages and update fValues */
            while (msgIterator.hasNext()) {
                DoubleArrayWritable recvMsg = msgIterator.next();
                int fValueIndex = 0;
                for (Double fVal : recvMsg) {
                    tempVals[fValueIndex++] += fVal / norm;
                }
            }
            for (int i = 0; i < vertexValue.getClassLabelIndexSize(); ++i) {
                vertexValue.setCurrentValue(i, tempVals[i]);
            }
            break;
        default:
            break;
        }
    }
}