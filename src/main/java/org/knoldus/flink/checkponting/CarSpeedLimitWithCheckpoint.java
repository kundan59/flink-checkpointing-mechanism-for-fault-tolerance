package org.knoldus.flink.checkponting;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * [[CarSpeedLimitWithCheckpoint]] class demonstrating the use case where on a particular highway if any car
 * crosses the speed limit, the system will generate an alert message that `you crossing the speed limit and the
 * average speed of few car that passed before you is [[this/55]] and show the car speed as well`. If a particular car
 * is under speed limit, the message would be- 'Thank you for staying under the speed limit `65`, your current speed is [[this]]
 *
 * This is achieved by keeping a ValueState which has Tuple of 2 element- the count of cars passed and the summation
 * of the speed of the cars passes right away.
 *
 * Here checkpointing is enabled with required configurations. If the application fails, on restarting it start from where
 * it left off.
 */
public final class CarSpeedLimitWithCheckpoint {

    public static void main(String[] args) throws Exception {

        //setup the stream execution environment
        final StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        //Triggering the checkpoint every 1 second
        executionEnvironment.enableCheckpointing(1000);

        //Every operator sees each record exactly once
        executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);//AT_LEAST_ONCE

        //Only one checkpoint operation be in progress at any point of time
        executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // at least 500ms of progress is observed between checkpoints
        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // checkpoint timeout for 50sec. if the checkpoint doesn't complete within 50sec discard that checkpoint
        executionEnvironment.getCheckpointConfig().setCheckpointTimeout( 60000 );

        executionEnvironment.setStateBackend(new FsStateBackend("file:///tmp/flink/checkpoints"));

        //Retain cancellation option for externalize checkpoint. maitain the checkpoint even if the job running has been canceld
        executionEnvironment.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION );


        //data stream- highway, car speed
        DataStream<Tuple2<String, Double>> carInfo =
                executionEnvironment.socketTextStream("localhost", 9000)
                        .map(carSpeed -> {
                            String[] carTokens = carSpeed.split(",");
                            return Tuple2.of(carTokens[0], Double.parseDouble(carTokens[1]));
                        }, TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
                        }));

        //key by highway
        DataStream<String> resultStream = carInfo
                .keyBy(((KeySelector<Tuple2<String, Double>, String>) carKeyBy ->
                        carKeyBy.f0), TypeInformation.of(new TypeHint<String>() {
                }))
                .flatMap(new AverageCarSpeedState());

        resultStream.print();

        executionEnvironment.execute("Checkpointing Mechanism");
    }

    private static class AverageCarSpeedState
            extends RichFlatMapFunction<Tuple2<String, Double>, String> {

        private transient ValueState<Tuple2<Integer, Double>> carCountAndSpeedState;

        @Override
        public void flatMap(Tuple2<String, Double> carDetail,
                            Collector<String> collector) throws Exception {

            if(carCountAndSpeedState.value() == null) {
                carCountAndSpeedState.update(Tuple2.of(0, 0.0));
            }
            checkCarAverageSpeed(carDetail, collector, carCountAndSpeedState.value());
        }

        private void checkCarAverageSpeed(Tuple2<String, Double> carDetail,
                                          Collector<String> collector,
                                          Tuple2<Integer, Double> carCountAndSpeed) throws IOException {

            if (carDetail.f1 >= 65) {

                collector.collect(String.format("You Exceeded! the average speed of the last %s cars," +
                                " which is %s, your speed is %s",
                        carCountAndSpeed.f0,
                        carCountAndSpeed.f1 / carCountAndSpeed.f0,
                        carDetail.f1));

                carCountAndSpeedState.clear();
                carCountAndSpeedState.update(Tuple2.of(0, 0.0));

            } else {
                collector.collect("Thank you for staying under the speed limit `65`, " +
                        "your current speed is: " + carDetail.f1);
            }
            carCountAndSpeedState.value().f0 += 1;
            carCountAndSpeedState.value().f1 += carDetail.f1;
            carCountAndSpeedState.update(carCountAndSpeed);
        }

        @Override
        public void open(Configuration parameters) {

            //Initializing ValueState object via ValueStateDescriptor
            // Parameters- name of value state: average-car-speed
            // Type information(what structure ValueState object accommodate):
            // TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>()) - count and average speed
            ValueStateDescriptor<Tuple2<Integer, Double>> averageSpeedValueStateDescriptor =
                    new ValueStateDescriptor<>("average-car-speed",
                            TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {
                            }));

            // get handle on ValueState Object
            carCountAndSpeedState = getRuntimeContext().getState(averageSpeedValueStateDescriptor);
        }
    }
}

