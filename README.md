# giraph.classifier

contains

- org.apache.giraph.classifier.lp.GFHF (Gaussian Fields and Harmonic Function by [X Zhu 2003])
- org.apache.giraph.classifier.MulticlassClassifierVertexInputFormat
- org.apache.giraph.classifier.MulticlassClassifierArgmaxVertexOutputFormat
- org.apache.giraph.classifier.MulticlassClassifierWritable

and others. (not completed yet).


## SYNOPSIS

    $ hadoop jar /tmp/giraph-0.70-jar-with-dependencies.jar org.apache.giraph.GiraphRunner \
      -DGFHF.maxSuperstep=30 -libjars /tmp/labelpropagation-1.0-SNAPSHOT.jar \
      org.apache.giraph.classifier.lp.GFHF \
      -if org.apache.giraph.classifier.MulticlassClassifierVertexInputFormat \
      -of org.apache.giraph.classifier.MulticlassClassifierArgmaxVertexOutputFormat \
      -w 1 -ip /user/smly/input3 -op /user/smly/output/gfhf01

    $ hadoop fs -cat /user/smly/input3/sample.txt
    1       [0.0, 0.0]      2       1.0     3       1.0
    2       [1.0, 0.0]      1       1.0     3       1.0
    3       [0.0, 0.0]      1       1.0     2       1.0     4       1.0
    4       [0.0, 0.0]      3       1.0     5       1.0     8       1.0
    5       [0.0, 0.0]      4       1.0     6       1.0     7       1.0
    6       [0.0, 1.0]      5       1.0     7       1.0
    7       [0.0, 0.0]      5       1.0     6       1.0
    8       [0.0, 0.0]      4       1.0     9       1.0
    9       [0.0, 1.0]      8       1.0

    $ hadoop fs -cat /user/smly/output/gfhf01/part-m-00001
    1       { label: U, argmax: 0, fval: [ 0.8941062029668139, 0.10587472354685788 ] }
    2       { label: L, argmax: 0, fval: [ 1.0, 0.0 ] }
    3       { label: U, argmax: 0, fval: [ 0.9529144735895161, 0.047055008832358816 ] }
    4       { label: U, argmax: 1, fval: [ 0.47056534711052384, 0.5293965059168197 ] }
    5       { label: U, argmax: 0, fval: [ 0.7529144735899415, 0.24705500883193343 ] }
    6       { label: L, argmax: 1, fval: [ 0.0, 1.0 ] }
    7       { label: U, argmax: 0, fval: [ 0.6941062029672393, 0.30587472354643247 ] }
    8       { label: U, argmax: 0, fval: [ 0.9411497677073759, 0.05881971471449907 ] }
    9       { label: L, argmax: 1, fval: [ 0.0, 1.0 ] }

