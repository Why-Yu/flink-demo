package flinkdemo.source;

import flinkdemo.entity.Query;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/*

 */
public class MySyntheticSource implements SourceFunction<Query> {
    private long count = 1L;
    private boolean isRunning = true;
    private final int boundSize;
    private final Random random;

    public MySyntheticSource(int boundSize) {
        this.boundSize= boundSize;
        this.random = new Random();
    }

    @Override
    public void run(SourceContext<Query> sourceContext) throws Exception {
        while (isRunning) {
            // 随机发送query的逻辑处理分区
            int partition = random.nextInt(3) + 1;
//            int partition = 1;
            // 如果不加1,生成的随机数区间是[0,boundSize)
            // 我们需要的是[1,boundSize + 1)
            int source = random.nextInt(boundSize) + 1;
            int target = random.nextInt(boundSize) + 1;
            // 避免生成source和target相同的query
            while (source == target) {
                target = random.nextInt(boundSize) + 1;
            }
            sourceContext.collect(new Query(partition, String.valueOf(source), String.valueOf(target)));
            Thread.sleep(50);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
