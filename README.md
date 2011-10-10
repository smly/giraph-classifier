# java-Giraph-LabelPropagation

The label propagation algorithm is one of the most basic semi-supervised model.
This is an implementation of the algorithm (GFHF, [Zhu and Ghahramani, 2002]) in Apache Giraph.

## Usage

Merge src/**/*.java into examples directiory.

    $ mvn copile && mvn package

    $ time hadoop jar target/giraph-0.70-jar-with-dependencies.jar org.apache.giraph.examples.LabelPropagationVertex -w 1 -l 2 -s 50 -i data -o output
    11/10/08 14:29:33 INFO mapred.JobClient: Running job: job_201110072115_0049
    11/10/08 14:29:34 INFO mapred.JobClient:  map 0% reduce 0%
    11/10/08 14:29:48 INFO mapred.JobClient:  map 50% reduce 0%
    11/10/08 14:29:51 INFO mapred.JobClient:  map 100% reduce 0%
    11/10/08 14:29:59 INFO mapred.JobClient: Job complete: job_201110072115_0049
    11/10/08 14:29:59 INFO mapred.JobClient: Counters: 168
    11/10/08 14:29:59 INFO mapred.JobClient:   Job Counters
    11/10/08 14:29:59 INFO mapred.JobClient:     Launched map tasks=2
    11/10/08 14:29:59 INFO mapred.JobClient:   Giraph Timers
    11/10/08 14:29:59 INFO mapred.JobClient:     Superstep 34 (milliseconds)=52
    11/10/08 14:29:59 INFO mapred.JobClient:     Superstep 64 (milliseconds)=42
    11/10/08 14:29:59 INFO mapred.JobClient:     Superstep 89 (milliseconds)=25
    ...

    11/10/08 14:29:59 INFO mapred.JobClient:   Giraph Stats11/10/08 14:29:59 INFO mapred.JobClient:     Aggregate edges=20
    11/10/08 14:29:59 INFO mapred.JobClient:     Superstep=15111/10/08 14:29:59 INFO mapred.JobClient:     Current workers=1
    11/10/08 14:29:59 INFO mapred.JobClient:     Current master task partition=011/10/08 14:29:59 INFO mapred.JobClient:     Sent messages=0
    11/10/08 14:29:59 INFO mapred.JobClient:     Aggregate finished vertices=9
    11/10/08 14:29:59 INFO mapred.JobClient:     Aggregate vertices=911/10/08 14:29:59 INFO mapred.JobClient:   FileSystemCounters
    11/10/08 14:29:59 INFO mapred.JobClient:     HDFS_BYTES_READ=28111/10/08 14:29:59 INFO mapred.JobClient:     HDFS_BYTES_WRITTEN=78061
    11/10/08 14:29:59 INFO mapred.JobClient:   Map-Reduce Framework11/10/08 14:29:59 INFO mapred.JobClient:     Map input records=2
    11/10/08 14:29:59 INFO mapred.JobClient:     Spilled Records=011/10/08 14:29:59 INFO mapred.JobClient:     Map output records=0
    hadoop jar target/giraph-0.70-jar-with-dependencies.jar   -w 1 -l 2 -s 50 -i data -o output  2.18s user 0.60s system 9% cpu 29.287 total

    $ hadoop fs -get output .
    $ cat output/part-m-00001 
    [1,["0.0","0.8705882278435371","0.12941175353001144"],[[2,1],[3,1]]]
    [2,["1.0","1.0","0.0"],[[1,1],[3,1]]]
    [3,["0.0","0.7411764594123644","0.2588235107853132"],[[1,1],[2,1],[4,1]]]
    [4,["0.0","0.352941161569427","0.6470588011776699"],[[3,1],[5,1],[8,1]]]
    [5,["0.0","0.14117645941236442","0.858823510785313"],[[4,1],[6,1],[7,1]]]
    [6,["2.0","0.0","1.0"],[[5,1],[7,1]]]
    [7,["0.0","0.07058822784353705","0.9294117535300114"],[[5,1],[6,1]]]
    [8,["0.0","0.17647057705942323","0.8235293931382544"],[[4,1],[9,1]]]
    [9,["2.0","0.0","1.0"],[[8,1]]]

## input format

[VertexID, ClassLabel (0 = unlabeled), Adjacencies]

    0$ cat data/sample.json  
    [1, 0, [[2, 1.0], [3, 1.0]]]
    [2, 1, [[1, 1.0], [3, 1.0]]]
    [3, 0, [[1, 1.0], [2, 1.0], [4, 1.0]]]
    [4, 0, [[3, 1.0], [5, 1.0], [8, 1.0]]]
    [5, 0, [[4, 1.0], [6, 1.0], [7, 1.0]]]
    [6, 2, [[5, 1.0], [7, 1.0]]]
    [7, 0, [[5, 1.0], [6, 1.0]]]
    [8, 0, [[4, 1.0], [9, 1.0]]]
    [9, 2, [[8, 1.0]]]

Vertices 1,3,4,5,7 and 8 are unalbeled.
Vertex 2 is labeled as '1'.
Vertex 7 and 9 are labeled as '2'.
Consecutive numbers must be assigned to vertices as labels.
The largest number of label must be given as parameter with '-l' option.

## output format

[VertexId,[ClassLabel (input),Function F(l)...],Adjacency]

    0$ hadoop fs -get output .
    0$ cat output/part-m-00001 
    [1,["0.0","0.8705882278435371","0.12941175353001144"],[[2,1],[3,1]]]
    [2,["1.0","1.0","0.0"],[[1,1],[3,1]]]
    [3,["0.0","0.7411764594123644","0.2588235107853132"],[[1,1],[2,1],[4,1]]]
    [4,["0.0","0.352941161569427","0.6470588011776699"],[[3,1],[5,1],[8,1]]]
    [5,["0.0","0.14117645941236442","0.858823510785313"],[[4,1],[6,1],[7,1]]]
    [6,["2.0","0.0","1.0"],[[5,1],[7,1]]]
    [7,["0.0","0.07058822784353705","0.9294117535300114"],[[5,1],[6,1]]]
    [8,["0.0","0.17647057705942323","0.8235293931382544"],[[4,1],[9,1]]]
    [9,["2.0","0.0","1.0"],[[8,1]]]

The result describes f_{l=1}(v_1) = 0.87, f_{l=2}(v_1) = 0.13, and argmax_l f_l(v_1) = 1.
So vertex 1 has been assigned the label of '1'.
In the same way, vertex 3 has been assigned the label of '1'
and vertices 4,5,7 and 8 have been assigned the label of '2'.

## References

- Chpelle O, Schölkopf B and Zien A: Semi-Supervised Learning, 508, MIT Press, Cambridge, MA, USA, (2006).
- http://mitpress.mit.edu/catalog/item/default.asp?ttype=2&tid=11015
