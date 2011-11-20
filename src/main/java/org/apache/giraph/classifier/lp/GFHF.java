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

    @Override
    public void compute(Iterator<DoubleArrayWritable> msgIterator) {
        if (getSuperstep() == 0) {
            LOG.debug("HOGE~");
            DoubleWritable[] dws = new DoubleWritable[1];
            DoubleArrayWritable daw = new DoubleArrayWritable();
            dws[0] = new DoubleWritable(Double.MAX_VALUE);
            daw.set(dws);
            setVertexValue(new MulticlassClassifierWritable(
                    new BooleanWritable(false), new IntWritable(1), daw));
        }
        voteToHalt();
    }
}
