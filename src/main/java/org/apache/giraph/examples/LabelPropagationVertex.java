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

package org.apache.giraph.examples;


import org.apache.giraph.graph.*;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.giraph.lib.TextVertexInputFormat.TextVertexReader;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.giraph.lib.TextVertexOutputFormat.TextVertexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;

import org.apache.giraph.examples.DoubleArrayWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
public class LabelPropagationVertex extends
        Vertex<LongWritable, DoubleArrayWritable, FloatWritable, DoubleArrayWritable>
        implements Tool {
    /** Configuration */
    private Configuration conf;
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(LabelPropagationVertex.class);
    /** The shortest paths id */
    public static String SOURCE_ID = "LabelPropagationVertex.sourceId";
    public static String MAX_SUPERSTEP = "LabelPropagationVertex.maxSuperstep";
    public static String MAX_LABELINDEX = "LabelPropagationVertex.maxLabelindex";
    /** Default shortest paths id */
    public static long SOURCE_ID_DEFAULT = 1;

    /**
     * Is this vertex the source id?
     *
     * @return True if the source id
     */
    private boolean isSource() {
        return (getVertexId().get() ==
            getContext().getConfiguration().getLong(SOURCE_ID,
                                                    SOURCE_ID_DEFAULT));
    }

    private long maxLabelIndex() {
    	return getContext().getConfiguration().getLong(MAX_LABELINDEX, 2);
    }
    
    @Override
    public void compute(Iterator<DoubleArrayWritable> msgIterator) {
    	if (getSuperstep() == 0) {
    		LOG.warn("MaxIter: " + getContext().getConfiguration().getLong(MAX_SUPERSTEP, 10));
    	}
        Long maxIteration = getContext().getConfiguration().getLong(MAX_SUPERSTEP, 10);
        if (getSuperstep() / 3 == maxIteration) {
        	LOG.warn(getVertexId().get());
        	DoubleWritable[] labels = (DoubleWritable[]) getVertexValue().get();
        	int label = (int) labels[0].get();
        	LOG.warn("LABEL: " + labels[0]);
        	DoubleWritable[] dws = new DoubleWritable[(int) maxLabelIndex() + 1];
        	dws[0] = new DoubleWritable(labels[0].get());
        	for (int i = 1; i <= maxLabelIndex(); ++i) {
        		double val = labels[i].get();
        		if (label > 0) {
        			dws[i] = new DoubleWritable(label == i ? 1.0 : 0.0);
        		} else {
       				dws[i] = new DoubleWritable(val);
        		}
        	}
        	DoubleArrayWritable daw = new DoubleArrayWritable();
        	daw.set(dws);
        	setVertexValue(daw);
        	voteToHalt();
        }
        if (getSuperstep() % 3 == 0) {
        	
        	LOG.warn(getVertexId().get());
        	DoubleWritable[] labels = (DoubleWritable[]) getVertexValue().get();
        	int label = (int) labels[0].get();
        	LOG.warn("LABEL: " + labels[0]);
        	DoubleWritable[] dws = new DoubleWritable[(int) maxLabelIndex() + 1];
        	dws[0] = new DoubleWritable(label);
        	for (int i = 1; i <= maxLabelIndex(); ++i) {
        		if (label > 0) {
        			dws[i] = new DoubleWritable(label == i ? 1.0 : 0.0);
        		} else {
        			if (getSuperstep() == 0) {
        				dws[i] = new DoubleWritable(0.0);
        			} else {
        				dws[i] = new DoubleWritable(labels[i].get());
        			}
        		}
        	}
        	DoubleArrayWritable daw = new DoubleArrayWritable();
        	daw.set(dws);
        	setVertexValue(daw);
        } else if (getSuperstep() % 3 == 1) {
        	LOG.info("SENDER: " + getVertexId());
        	Double vals = 0.0;
        	DoubleWritable[] labels = (DoubleWritable[]) getVertexValue().get();
        	for (int i = 1; i <= maxLabelIndex(); ++i) {
        		vals += labels[i].get();
        	}
        	if (vals > 0.0) {
        		for (LongWritable targetVertexId : this) {
        			FloatWritable edgeValue = getEdgeValue(targetVertexId);
        			LOG.info("Vertex " + getVertexId() + " sent to " +
        					targetVertexId + "...");
        			DoubleArrayWritable daw = new DoubleArrayWritable();
        			DoubleWritable[] dws = new DoubleWritable[(int) maxLabelIndex() + 1];
        			dws[0] = new DoubleWritable(0.0); // dummy
        			for (int i = 1; i <= maxLabelIndex(); ++i) {
        				dws[i] = new DoubleWritable(labels[i].get() * edgeValue.get());
        			}
        			daw.set(dws);
        			sendMsg(targetVertexId, daw);
        		}
        	}
        } else if (getSuperstep() % 3 == 2) {
        	// vertexDegree should be included in vertexValue....
        	double deg = 0.0;
        	for (LongWritable targetVertexId : this) {
        		FloatWritable edgeValue = getEdgeValue(targetVertexId);
        		deg += edgeValue.get();
        	}
        	DoubleWritable[] labels = (DoubleWritable[]) getVertexValue().get();
        	Double[] vals = new Double[(int)maxLabelIndex() + 1];
        	vals[0] = labels[0].get();
        	for (int i = 0; i <= maxLabelIndex(); ++i) {
        		//vals[i] = labels[i].get();
        		if (i == 0) {
        			vals[i] = labels[i].get();
        		} else {
        			vals[i] = 0.0;
        		}
        	}
        	while (msgIterator.hasNext()) {
        		DoubleArrayWritable recvMsg = msgIterator.next();
        		DoubleWritable[] dws = (DoubleWritable[]) recvMsg.get();
        		LOG.info("STEP3 => " + getVertexId() + " reqv msg!");
        		for (int i = 1; i <= maxLabelIndex(); ++i) {
        			vals[i] += dws[i].get() / deg;
        		}
        		LOG.info("Updated: " + getVertexId() + " value as ["+vals[0]+","+vals[1]+","+vals[2]+"]");
        		
        	}
        	for (int i = 0; i <= maxLabelIndex(); ++i) {
        		labels[i] = new DoubleWritable(vals[i]);
        	}
        	DoubleArrayWritable daw = new DoubleArrayWritable();
        	daw.set(labels);
        	LOG.info("Set: " + getVertexId() + " value as ["+labels[0]+","+labels[1]+","+labels[2]+"]");
        	setVertexValue(daw);
        	if (labels[0].get() < 1.0) {
        		//voteToHalt(); // !!!
        	}
        }
    }

    /**
     * VertexInputFormat that supports {@link LabelPropagationVertex}
     */
    public static class LabelPropagationVertexInputFormat extends
            TextVertexInputFormat<LongWritable, DoubleArrayWritable, FloatWritable> {
        @Override
        public VertexReader<LongWritable, DoubleArrayWritable, FloatWritable>
                createVertexReader(InputSplit split,
                                   TaskAttemptContext context)
                                   throws IOException {
            return new LabelPropagationVertexReader(
                textInputFormat.createRecordReader(split, context));
        }
    }

    /**
     * VertexReader that supports {@link LabelPropagationVertex}.  In this
     * case, the edge values are not used.  The files should be in the
     * following JSON format:
     * JSONArray(<vertex id>, <vertex value>,
     *           JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
     * Here is an example with vertex id 1, vertex value 4.3, and two edges.
     * First edge has a destination vertex 2, edge value 2.1.
     * Second edge has a destination vertex 3, edge value 0.7.
     * [1,4.3,[[2,2.1],[3,0.7]]]
     */
    public static class LabelPropagationVertexReader extends
            TextVertexReader<LongWritable, DoubleArrayWritable, FloatWritable> {

        public LabelPropagationVertexReader(
                RecordReader<LongWritable, Text> lineRecordReader) {
            super(lineRecordReader);
        }

        @Override
        public boolean next(MutableVertex<LongWritable,
                            DoubleArrayWritable, FloatWritable, ?> vertex)
                throws IOException, InterruptedException {
            if (!getRecordReader().nextKeyValue()) {
                return false;
            }

            Text line = getRecordReader().getCurrentValue();
            try {
                JSONArray jsonVertex = new JSONArray(line.toString());
                // label index only
                DoubleArrayWritable daw = new DoubleArrayWritable();
                DoubleWritable dws[] = new DoubleWritable[1];
                dws[0] = new DoubleWritable(jsonVertex.getDouble(1));
                daw.set(dws);
                vertex.setVertexId(
                    new LongWritable(jsonVertex.getLong(0)));
                vertex.setVertexValue(daw);
                JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
                for (int i = 0; i < jsonEdgeArray.length(); ++i) {
                    JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
                    vertex.addEdge(new LongWritable(jsonEdge.getLong(0)),
                            new FloatWritable((float) jsonEdge.getDouble(1)));
                }
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "next: Couldn't get vertex from line " + line, e);
            }
            return true;
        }
    }

    /**
     * VertexOutputFormat that supports {@link LabelPropagationVertex}
     */
    public static class LabelPropagationVertexOutputFormat extends
            TextVertexOutputFormat<LongWritable, DoubleArrayWritable,
            FloatWritable> {

        @Override
        public VertexWriter<LongWritable, DoubleArrayWritable, FloatWritable>
                createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter =
                textOutputFormat.getRecordWriter(context);
            return new LabelPropagationVertexWriter(recordWriter);
        }
    }

    /**
     * VertexWriter that supports {@link LabelPropagationVertex}
     */
    public static class LabelPropagationVertexWriter extends
            TextVertexWriter<LongWritable, DoubleArrayWritable, FloatWritable> {
        public LabelPropagationVertexWriter(
                RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(BasicVertex<LongWritable, DoubleArrayWritable,
                                FloatWritable, ?> vertex)
                throws IOException, InterruptedException {
            JSONArray jsonVertex = new JSONArray();
            try {
                jsonVertex.put(vertex.getVertexId().get());
                jsonVertex.put(vertex.getVertexValue().get());
                JSONArray jsonEdgeArray = new JSONArray();
                for (LongWritable targetVertexId : vertex) {
                    JSONArray jsonEdge = new JSONArray();
                    jsonEdge.put(targetVertexId.get());
                    jsonEdge.put(vertex.getEdgeValue(targetVertexId).get());
                    jsonEdgeArray.put(jsonEdge);
                }
                jsonVertex.put(jsonEdgeArray);
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "writeVertex: Couldn't write vertex " + vertex);
            }
            getRecordWriter().write(new Text(jsonVertex.toString()), null);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] argArray) throws Exception {
    	Options options = new Options();
    	options.addOption("h", "help", false, "Help");
    	options.addOption("v", "verbose", false, "Verbose");
    	options.addOption("w", "workers", true, "Number of workers");
    	options.addOption("s", "supersteps", true, "Supersteps to execute before");
    	options.addOption("l", "labels", true, "Number of labels");
    	options.addOption("i", "input", true, "Input");
    	options.addOption("o", "output", true, "Output");
    	HelpFormatter formatter = new HelpFormatter();
    	if (argArray.length == 0) {
    		formatter.printHelp(getClass().getName(), options, true);
    		return 0;
    	}
    	CommandLineParser parser = new PosixParser();
    	CommandLine cmd = parser.parse(options, argArray);
    	if (cmd.hasOption('h')) {
    		formatter.printHelp(getClass().getName(), options, true);
    		return 0;
    	}
    	if (!cmd.hasOption('w')) {
    		System.out.println("Need to choose the number of workers");
    		return -1;
    	}
    	if (!cmd.hasOption('s')) {
    		System.out.println("Need to set the number of supersteps");
    		return -1;
    	}
    	if (!cmd.hasOption('l')) {
    		System.out.println("Need to set the number of class labels");
    		return -1;
    	}
    	if (!cmd.hasOption('i')) {
    		System.out.println("Need to choose the input");
    		return -1;
    	}
    	if (!cmd.hasOption('o')) {
    		System.out.println("Neeed to choose the output");
    		return -1;
    	}
    	int workers = Integer.parseInt(cmd.getOptionValue('w'));
    	GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    	job.setVertexClass(getClass());
        job.setVertexInputFormatClass(
            LabelPropagationVertexInputFormat.class);
        job.setVertexOutputFormatClass(
            LabelPropagationVertexOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(cmd.getOptionValue('i')));
        FileOutputFormat.setOutputPath(job, new Path(cmd.getOptionValue('o')));
        job.getConfiguration().setLong(LabelPropagationVertex.MAX_SUPERSTEP,
        								Long.parseLong(cmd.getOptionValue('s')));
        job.getConfiguration().setLong(LabelPropagationVertex.MAX_LABELINDEX,
				Long.parseLong(cmd.getOptionValue('l')));
        job.setWorkerConfiguration(workers, workers, 100.0f);
        if (job.run(true) == true) {
        	return 0;
        } else {
        	return 1;
        }
    	/*
        if (argArray.length != 6) {
            throw new IllegalArgumentException(
                "run: Must have 6 arguments <input path> <output path> " +
                "<source vertex id> <# of workers> <# of supersteps> <# of label>");
        }
        GiraphJob job = new GiraphJob(getConf(), getClass().getName());
        job.setVertexClass(getClass());
        job.setVertexInputFormatClass(
            LabelPropagationVertexInputFormat.class);
        job.setVertexOutputFormatClass(
            LabelPropagationVertexOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(argArray[0]));
        FileOutputFormat.setOutputPath(job, new Path(argArray[1]));
        job.getConfiguration().setLong(LabelPropagationVertex.SOURCE_ID,
                                       Long.parseLong(argArray[2]));
        job.getConfiguration().setLong(LabelPropagationVertex.MAX_SUPERSTEP,
        								Long.parseLong(argArray[4]));
        job.getConfiguration().setLong(LabelPropagationVertex.MAX_LABELINDEX,
        								Long.parseLong(argArray[5]));
        job.setWorkerConfiguration(Integer.parseInt(argArray[3]),
                                   Integer.parseInt(argArray[3]),
                                   100.0f);
        if (job.run(true) == true) {
            return 0;
        } else {
            return -1;
        }
        */
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new LabelPropagationVertex(), args));
    }
}
