package flinkdemo.operator;

import flinkdemo.entity.Query;
import flinkdemo.util.PathCalculator;
import org.apache.flink.api.common.functions.MapFunction;

/*
解决miniQuery的简单算子，只需要单纯使用A*，因为query都比较小，缓存的意义也不大
 */
public class MiniResolver implements MapFunction<Query, String> {
    @Override
    public String map(Query query) throws Exception {
        PathCalculator pathCalculator = new PathCalculator();
        return pathCalculator.getAstarShortestPath(query).toString();
    }
}
