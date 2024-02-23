
package org.kiba.learning_ml;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

@Slf4j
/**
 * KMeans（K-Means Clustering）是一种聚类算法，它将数据样本划分为 K 个簇。KMeans 算法的基本思想是通过迭代将样本分配到不同的簇中，使得每个样本到其所属簇的中心的距离最小。
 * */
public class KmeansExample {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Generates input training and prediction data.
        DataStream<Row> trainStream = env.fromElements(
                        Row.of(Vectors.dense(2.0, 3.0)),
                        Row.of(Vectors.dense(2.1, 3.1)),
                        Row.of(Vectors.dense(200.1, 300.1)),
                        Row.of(Vectors.dense(200.2, 300.2)),
                        Row.of(Vectors.dense(200.3, 300.3)),
                        Row.of(Vectors.dense(200.4, 300.4)),
                        Row.of(Vectors.dense(200.4, 300.4)),
                        Row.of(Vectors.dense(200.6, 300.6)),
                        Row.of(Vectors.dense(2.1, 3.1)),
                        Row.of(Vectors.dense(2.1, 3.1)),
                        Row.of(Vectors.dense(2.1, 3.1)),
                        Row.of(Vectors.dense(2.1, 3.1)),
                        Row.of(Vectors.dense(2.3, 3.2)),
                        Row.of(Vectors.dense(2.3, 3.2)),
                        Row.of(Vectors.dense(2.8, 3.2)),
                        Row.of(Vectors.dense(300., 3.2)),
                        Row.of(Vectors.dense(2.2, 3.2)),
                        Row.of(Vectors.dense(2.4, 3.2)),
                        Row.of(Vectors.dense(2.5, 3.2)),
                        Row.of(Vectors.dense(2.5, 3.2)),
                        Row.of(Vectors.dense(2.1, 3.1)));

        Table trainTable = tEnv.fromDataStream(trainStream).as("features");

        // Creates a K-means object and initializes its parameters.
        KMeans kmeans = new KMeans().setK(2).setSeed(1L);

        // Trains the K-means Model.
        KMeansModel kmeansModel = kmeans.fit(trainTable);
        // Uses the K-means Model for predictions.
        Table outputTable = kmeansModel.transform(trainTable)[0];


        // Extracts and displays the results.
        for (CloseableIterator<Row> it = outputTable.execute().collect(); it.hasNext(); ) {
            Row row = it.next();
            DenseVector features = (DenseVector) row.getField(kmeans.getFeaturesCol());
            int clusterId = (Integer) row.getField(kmeans.getPredictionCol());
            System.out.printf("========Features: %s \tCluster ID: %s\n", features, clusterId);
        }


    }
}
