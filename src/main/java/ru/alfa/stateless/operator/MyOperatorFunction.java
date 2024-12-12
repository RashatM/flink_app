package ru.alfa.stateless.operator;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;


import java.util.ArrayList;
import java.util.List;

public class MyOperatorFunction extends RichFlatMapFunction<String, String>
        implements CheckpointedFunction {

    private transient ListState<String> checkpointedState;
    private List<String> localBuffer;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        localBuffer = new ArrayList<>();
    }

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        localBuffer.add(value);
        if (localBuffer.size() == 10) {
            for (String s : localBuffer) {
                out.collect(s);
            }
            localBuffer.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (String s : localBuffer) {
            checkpointedState.add(s);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<String> descriptor =
                new ListStateDescriptor<>("buffer", String.class);
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (String s : checkpointedState.get()) {
                localBuffer.add(s);
            }
        }
    }
}


