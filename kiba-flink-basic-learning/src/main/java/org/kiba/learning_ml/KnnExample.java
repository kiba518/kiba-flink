

package org.kiba.learning_ml;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.ml.classification.knn.Knn;
import org.apache.flink.ml.classification.knn.KnnModel;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

@Slf4j
/** Simple program that trains a Knn model and uses it for classification.
 * Knn（K-Nearest Neighbors）是一种分类算法，它根据样本与其最近的邻居的类别来对新样本进行分类。Knn 算法通过计算新样本与训练数据集中每个样本的距离，然后选择最近的 K 个样本，根据这些邻居的类别来确定新样本的类别。
 * */
public class KnnExample {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Generates input training and prediction data.
        DataStream<Row> trainStream =
                env.fromElements(
                        Row.of(Vectors.dense(2.0, 3.0), 1.0),
                        Row.of(Vectors.dense(2.1, 3.1), 1.0),
                        Row.of(Vectors.dense(200.1, 300.1), 2.0),
                        Row.of(Vectors.dense(200.2, 300.2), 2.0),
                        Row.of(Vectors.dense(200.3, 300.3), 2.0),
                        Row.of(Vectors.dense(200.4, 300.4), 2.0),
                        Row.of(Vectors.dense(200.4, 300.4), 2.0),
                        Row.of(Vectors.dense(200.6, 300.6), 2.0),
                        Row.of(Vectors.dense(2.1, 3.1), 1.0),
                        Row.of(Vectors.dense(2.1, 3.1), 1.0),
                        Row.of(Vectors.dense(2.1, 3.1), 1.0),
                        Row.of(Vectors.dense(2.1, 3.1), 1.0),
                        Row.of(Vectors.dense(2.3, 3.2), 1.0),
                        Row.of(Vectors.dense(2.3, 3.2), 1.0),
                        Row.of(Vectors.dense(2.8, 3.2), 3.0),
                        Row.of(Vectors.dense(300., 3.2), 4.0),
                        Row.of(Vectors.dense(2.2, 3.2), 1.0),
                        Row.of(Vectors.dense(2.4, 3.2), 5.0),
                        Row.of(Vectors.dense(2.5, 3.2), 5.0),
                        Row.of(Vectors.dense(2.5, 3.2), 5.0),
                        Row.of(Vectors.dense(2.1, 3.1), 1.0));
        Table trainTable = tEnv.fromDataStream(trainStream).as("features", "label");//为表格中的列指定别名

        DataStream<Row> predictStream =
                env.fromElements(
                        Row.of(Vectors.dense(4.0, 4.1), 5.0), Row.of(Vectors.dense(300, 42), 2.0));
        Table predictTable = tEnv.fromDataStream(predictStream).as("features", "label");

        // Creates a Knn object and initializes its parameters.
        Knn knn = new Knn().setK(4);

        // Trains the Knn Model.
        KnnModel knnModel = knn.fit(trainTable);

        Table outputTable1 = knnModel.transform(trainTable)[0];
        // Extracts and displays the results.
        for (CloseableIterator<Row> it = outputTable1.execute().collect(); it.hasNext(); ) {
            Row row = it.next();
            DenseVector features = (DenseVector) row.getField(knn.getFeaturesCol());//源数据
            double clusterId = (Double) row.getField(knn.getPredictionCol());//聚集点
            System.out.printf("===========属性Features: %s  Cluster ID: %s ========== \n", features, clusterId);
        }



        // Uses the Knn Model for predictions.
        Table outputTable2 = knnModel.transform(predictTable)[0];

        // Extracts and displays the results.
        for (CloseableIterator<Row> it = outputTable2.execute().collect(); it.hasNext(); ) {
            Row row = it.next();
            DenseVector features = (DenseVector) row.getField(knn.getFeaturesCol());
            double expectedResult = (Double) row.getField(knn.getLabelCol());
            double predictionResult = (Double) row.getField(knn.getPredictionCol());

//            Log.info("features:"+features);
//            Log.info("expectedResult:"+expectedResult);
//            Log.info("predictionResult:"+predictionResult);
//            Log.info("========================");
//            System.out.printf(  "========================" );
            System.out.printf(
                    "========================Features: %-15s \tExpected Result: %s \tPrediction Result: %s\n",
                    features, expectedResult, predictionResult);
        }
    }
}
