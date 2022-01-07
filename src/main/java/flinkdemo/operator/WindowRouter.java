package flinkdemo.operator;

import com.google.common.geometry.S1ChordAngle;
import com.google.common.geometry.S2Cap;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import flinkdemo.entity.Query;
import flinkdemo.entity.Vertex;
import flinkdemo.util.ParametersPasser;
import flinkdemo.util.TopologyGraph;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
因为source我们自己设置了三个逻辑分区，所以实际上这个算子维护着三份不同的状态
当然这个算子实际上是由许多的算子子任务组成的
!!!功能：依据query的角度和长度，对query进行分区以及数据流上的分流
 */
public class WindowRouter extends KeyedProcessFunction<Integer, Query, Query> {
    public static final Logger logger = LoggerFactory.getLogger(WindowRouter.class);

    private static final long HALF_HOUR = 30 * 60 * 1000;
    private final OutputTag<Query> miniQuery;

    // bufferSize还不能设置为transient,否则序列化后会变成0，但是处理过程中会序列化吗，具体原因不明
    private final int bufferSize;
    // f0表示第几轮次,f1表示本轮次的第几个query
    private transient ValueState<Tuple2<Integer, Integer>> countState;
    // 之前所有轮次的平均值
    private transient ValueState<Double> averageState;
    // 批量更新的缓存位置
    private transient ValueState<Double> bufferLengthState;

    // ******非常重要的总结
    // 问题在于，我们设置了三个逻辑分区，而有两个分区跑在一个线程上，一个分区跑在另一个线程上
    // 共享线程的逻辑分区实际上会共用这个类变量，依据我们slot的知识推测，每个slot都会对算子生成一个实例，所以keyBy后，若一个线程有两个逻辑分区的算子在上面运行，就会争抢这个类变量
    // 这也就是示例代码中，在processElement中每次创建新的变量接受State.value()，而不是使用类变量来接受value[看起来使用类变量效率更高，但这样会造成不可预知的被另一个逻辑分区修改]
    // 所以最后导致生成了两个计时器，因为两个线程两个实例，即使有三个逻辑分区(不是我之前以为的操作原子性问题导致的重复注册)
    // 但是State却是有三份的，是和逻辑分区保持一致的,如果使用类变量那么共享线程那两个逻辑分区，谁先抢到这个变量就只会在那个逻辑分区上建立计时器，即究竟在2还是在3上建立是不可预知的了
    private transient ValueState<Boolean> firstState;

    // 可以通过构造函数传参，也可以先全局注册，然后在open里面获取全局注册的parameterTool
    // 我们统一通过构造函数吧，这样比较简洁，代码负担也较小
    public WindowRouter(OutputTag<Query> miniQuery, int bufferSize) {
        this.bufferSize = bufferSize;
        this.miniQuery = miniQuery;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<Integer, Integer>> countStateDescriptor = new ValueStateDescriptor<>("roundCount",
                TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {}), Tuple2.of(0, 0));
        // 以路网最小外接矩形对角线的35%作为初始average
        ValueStateDescriptor<Double> averageStateDescriptor = new ValueStateDescriptor<Double>("average", Types.DOUBLE,
                ParametersPasser.initialAverage);
        ValueStateDescriptor<Double> bufferLengthStateDescriptor = new ValueStateDescriptor<Double>("bufferLength", Types.DOUBLE, 0.0);
        ValueStateDescriptor<Boolean> firstStateDescriptor = new ValueStateDescriptor<Boolean>("routerFirst", Types.BOOLEAN, true);
        countState = getRuntimeContext().getState(countStateDescriptor);
        averageState = getRuntimeContext().getState(averageStateDescriptor);
        bufferLengthState = getRuntimeContext().getState(bufferLengthStateDescriptor);
        firstState = getRuntimeContext().getState(firstStateDescriptor);
    }

    @Override
    public void processElement(Query query, Context context, Collector<Query> collector) throws Exception {
        // 如果是第一次运行,注册一个定时器,目的是防止表示轮次的state:count.f0一直无限制增大，所以每隔一小时除2
        // 其实也可以在此函数中在固定时间注册一个计时器，但这样每个元素处理的时候都会想要注册定时器
        // 那么就需要timeService内部在好几层调用之后,判断计时器是否相同,还不如我们在这里直接判断,减少几层调用堆栈
        if (firstState.value()) {
            firstState.update(false);
            logger.info(context.getCurrentKey() + "第一次注册");
            long timer = context.timerService().currentProcessingTime();
            context.timerService().registerProcessingTimeTimer(timer + HALF_HOUR);
        }

        // query添加必要属性
        Vertex source = TopologyGraph.getVertex(query.getSourceID());
        Vertex target = TopologyGraph.getVertex(query.getTargetID());
        // 虽然我们的TopologyGraph中确实存有距离，但是邻接链表的查找需要遍历，所以我们在这里直接计算好了
        // (在几十公里的尺度下，其实和球面距离相差不是很多的)
        // 另外在后续的S1ChordAngle也可以直接使用
        double distance = source.getDistance(target);
        query.setSource(source);
        query.setTarget(target);
        query.setLength(distance);

        // route
        // direction window router(依据方向窗口进行逻辑流划分)
        int windowNumber = getThetaWindow(source, target);
        query.setPartition(windowNumber);

        // Length window router(利用长度筛选出短距离请求单独直接处理，对长度距离设置为可缓存，大部分普通距离则正常后续处理)
        double average = averageState.value();
        if (distance < 0.18 * average) {
            context.output(miniQuery, query);
        } else {
            // 最开始设置的值是1.6 * average但是这样貌似要填充完一个cache要花费好长时间
            // 所以我们把阈值设小一些，好让cache快点填充完毕，代价是后续算子每个query需要多几次比较来查看是否可以add cache
            // 注意！！错误！！cacheable应该根据query所属的cluster大小来决定，而不是所有query的平均大小
            // 不然小cluster中，就会一直没有缓存
//            if (distance > average) {
//                query.setCacheable(true);
//            }
            collector.collect(query);
        }

        // 批量维护最关键的利用长度分发请求的判别依据:average
        Tuple2<Integer, Integer> count= countState.value();
        double bufferLength = bufferLengthState.value();
        // 如果还在批处理允许的缓存数量内,average不用更新,简单的更新一下count.f1和buffer
        if (count.f1 < bufferSize) {
            count.f1 += 1;
            countState.update(count);
            bufferLengthState.update(bufferLength + distance);
        } else {
            // 此轮缓存已满,重置轮次,更新average,并把当前的这个元素放到缓存里
            // 因为这一步需要计算很多数据，所以我们才进行批量式更新，而不是来一个query就更新一次
            count.f0 += 1;
            count.f1 = 1;
            double latestAverage = average + (((bufferLength / bufferSize) - average) / count.f0);
            averageState.update(latestAverage);
            bufferLength = distance;
            countState.update(count);
            bufferLengthState.update(bufferLength);

            // 计算下一个算子中聚簇应该达到的区分粒度，并相应更新
            // 需要注意的是latestAverage / 4来表示半径 和 source与query两点的center做半径，长度是不一样的
            // 因为计算是基于球面几何的原因，使用latestAverage / 4会导致生成的Cap略微小一些
            // 不过在100公里的尺度下，两个Cap的面积大概只有(4.75E-4)%的误差，我觉得是可以忽略的
            // (并且在误差如此小的情况下，通过latestAverage / 4替代，而不是使用精确的半径，我们可以节省大量计算)
            // 由于cap显著比椭圆面积大，所以我们对半径再除2变成latestAverage / 16
            if (context.getCurrentKey() == 1) {
                double length2 = Math.pow(latestAverage, 2);
                S2Cap s2Cap = S2Cap.fromAxisChord(new S2Point(1.0, 0.0, 0.0),
                        S1ChordAngle.fromLength2(length2 / 16));
                int granularity = TopologyGraph.getGranularity(s2Cap);
                // QueryCluster中，query匹配cluster应该采取的粒度
                ParametersPasser.setGranularity(granularity);
                // CacheAndLandmark中，partial hit搜索最近的缓存path中的点的范围
                // 范围控制在query长度的2%以内
                ParametersPasser.setMaxDistance(length2 * 4E-4);
                logger.info("average更新:" + latestAverage + ",granularity更新" + granularity);
            }
        }
    }

    /*
    维护count.f0不超限,并重新生成下一次计时器
    */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Query> out) throws Exception {
        // 维护count.f0不超限
        Tuple2<Integer, Integer> count = countState.value();
        count.f0 = count.f0 / 2;
        countState.update(count);
        ctx.timerService().registerProcessingTimeTimer(timestamp + HALF_HOUR);
    }

    /*
    计算方向不可以使用两点(三维向量)之间的差作为方向向量
    这样会导致差向量是一个圆锥形面上的所有query都得出同一个结果，但是这些query方向应该是不同的
    因为我们把方向定义三维向量在二维基准投影面上的夹角(在小尺度上，路网显然是近似为二维的，投影为墨卡托投影)
     */
    private int getThetaWindow(Vertex source, Vertex target) {
        S2LatLng s2LatLngS = new S2LatLng(source);
        S2LatLng s2LatLngT = new S2LatLng(target);

        double latDiff = s2LatLngS.latRadians() - s2LatLngT.latRadians();
        double lngDiff = s2LatLngS.lngRadians() - s2LatLngT.lngRadians();
        // 计算墨卡托投影后两点与x轴的夹角
        double theta = lngDiff / Math.sqrt(Math.pow(lngDiff, 2) + Math.pow(latDiff, 2));
        // 向量的两个方向被我们视为同一个方向，一三象限夹角视为正(<90)，二四象限夹角视为负(>90)
        // 这样就等于落在三四象限的向量被我们取反方向
        if (latDiff * lngDiff >= 0) {
            theta = Math.abs(theta);
        } else {
            if (theta > 0) {
                theta = -theta;
            }
        }
        return TopologyGraph.getThetaWindow(theta);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
