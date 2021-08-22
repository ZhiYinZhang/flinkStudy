package datastream.faultTolerant;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class chekpointDemo {
    public static void main(String[] args){
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpoint,设置间隔
        senv.enableCheckpointing(5000);

        //设置重启策略，尝试3次，每次间隔3秒
        senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));
//        senv.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000));

        senv.setStateBackend(new FsStateBackend("file://e://data//flink_backend"));

    }
}
