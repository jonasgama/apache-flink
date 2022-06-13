package com.flink.checkpoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckpointSample {

    public static void main(String[] args)
            throws Exception{

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);

        // to set minimum progress time to happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // checkpoints have to complete within 10000 ms, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(10000);

        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);  // AT_LEAST_ONCE

        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // enable externalized checkpoints which are retained after job cancellation
        //even when the job is cancelled, it will keep the checkpoint in case of RETAIN_ON_CANCELLATION
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);  // DELETE_ON_CANCELLATION

        //
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(	3, 100 ));
        // number of restart attempts , delay in each restart


    }
}
