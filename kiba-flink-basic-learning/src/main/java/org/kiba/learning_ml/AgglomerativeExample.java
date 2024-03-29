

package org.kiba.learning_ml;
import org.apache.flink.ml.clustering.agglomerativeclustering.AgglomerativeClustering;
import org.apache.flink.ml.clustering.agglomerativeclustering.AgglomerativeClusteringParams;
import org.apache.flink.ml.common.distance.EuclideanDistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/**
 * 层次聚类
 * https://nightlies.apache.org/flink/flink-ml-docs-master/docs/operators/clustering/agglomerativeclustering/
 * Simple program that creates an AgglomerativeClustering instance and uses it for clustering. */
public class AgglomerativeExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Generates input data.
        DataStream<DenseVector> inputStream =
                env.fromElements(
                        Vectors.dense(1, 1),
                        Vectors.dense(1, 4),
                        Vectors.dense(1, 0),
                        Vectors.dense(4, 1.5),
                        Vectors.dense(4, 4),
                        Vectors.dense(4, 0));
        Table inputTable = tEnv.fromDataStream(inputStream).as("features");

        // Creates an AgglomerativeClustering object and initializes its parameters.
        AgglomerativeClustering agglomerativeClustering =
                new AgglomerativeClustering()
                        .setLinkage(AgglomerativeClusteringParams.LINKAGE_WARD)
                        .setDistanceMeasure(EuclideanDistanceMeasure.NAME)
                        .setPredictionCol("prediction");

        // Uses the AgglomerativeClustering object for clustering.
        Table[] outputs = agglomerativeClustering.transform(inputTable);

        // Extracts and displays the results.
        for (CloseableIterator<Row> it = outputs[0].execute().collect(); it.hasNext(); ) {
            Row row = it.next();
            DenseVector features =
                    (DenseVector) row.getField(agglomerativeClustering.getFeaturesCol());
            int clusterId = (Integer) row.getField(agglomerativeClustering.getPredictionCol());
            System.out.printf("Features: %s \tCluster ID: %s\n", features, clusterId);
        }
    }
}