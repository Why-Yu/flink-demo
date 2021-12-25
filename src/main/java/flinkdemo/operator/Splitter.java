package flinkdemo.operator;

import flinkdemo.entity.Query;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class Splitter implements FlatMapFunction<Query, Query> {
    @Override
    public void flatMap(Query s, Collector<Query> collector) throws Exception {
        collector.collect(s);
    }
}
