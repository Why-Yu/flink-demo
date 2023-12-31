package flinkdemo.operator;

import com.google.common.geometry.*;
import flinkdemo.entity.Path;
import flinkdemo.entity.Query;
import flinkdemo.entity.TreeNode;
import flinkdemo.util.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*
!!!功能：核心算子，提供三个主要功能
1、cache的构建与匹配(path map, inverted map, radix tree), 提供完全匹配和部分匹配
2、cache更新(基于LFU-aging)与废弃
3、基于localLandmark的ALT计算
奇怪的地方，同一个算子完成的功能太多了，很希望能拆分，但感觉又没有办法拆开，导致这个算子拥有16个状态，总觉得不太好
 */
public class CacheAndLandmark extends KeyedProcessFunction<String, Query, String> {
    public static final Logger logger = LoggerFactory.getLogger(CacheAndLandmark.class);

    // !!! 注意，我们在这里不能使用Flink 的TTL功能(因为这样不仅State会丢失默认值，
    // 并且state中的每个元素都会额外存储一个包括用户状态以及时间戳的 Java 对象，一万个元素就会有一万个，大大增加了内存的使用量，以及计算压力
    // 以至于多花费以GB计算的内存和100%的CPU占用)
    // 所以对于遗弃的cluster我们自己定义遗弃的标准，然后把状态清空
    // 设置缓存过期清除策略，含义为数据的有效期是1分钟，在读或者写时都会刷新状态的时间戳
    // 在数据过期后，不管物理上是否被删除，都不再返回
//    private static final StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.minutes(1))
//            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//            .build();
    // 注册的计时器时间窗口
    private static final long ONE_MINUTE = 30 * 1000;
    // 为防止计时器在某一个时刻集中运行，利用随机数制造一个时间小偏差
    private static final Random random = new Random();
    // 关于localCache的一些可设置参数，具体解释见myJob.properties
    private final int winnersMaxSize;
    private final int convergence;
    private final double negligible;
    private final int abandon;
    private final int qualified;
    private final int hotCluster;

    // --- localCache
    // Path表，提供pathID到path的查找
    private transient MapState<Integer, Path> winnersPathState;
    // inverted map，提供某个节点在某条或多条缓存路径序列上对应的位置(方便我们无需遍历，直接截取部分路径序列输出即可)
    private transient MapState<String, ArrayList<Tuple2<Integer, Integer>>> winnersVertexState;
    // 提供partial hit功能，RadixTree存储了当前聚簇下所有缓存路径覆盖的格网
    private transient ValueState<RadixTree> radixTreeState;
    // 现有缓存大小
    private transient ValueState<Integer> winnersSizeState;
    // 当前cluster的最大缓存容量
    private transient ValueState<Integer> hotClusterSizeState;
    // ID提供器
    private transient ValueState<Integer> IDSupplierState;

    // --- localLandmark
    // localLandmark表，两个local landmark 提供A*算法中，更tighter的lower bound，让最短路径算法的搜索空间尽可能小
    private transient MapState<String, Double> sourceLandmarkState;
    private transient MapState<String, Double> targetLandmarkState;
    // owner Cluster，存储创建当前cluster的query，为聚簇合格之后计算landmark制造条件
    private transient ValueState<Query> ownerClusterState;

    // --- hit ratio
    // 统计当前聚簇下的，本轮次时间窗口内的query总数，提供后续hit ratio计算
    private transient ValueState<Integer> queryNumberState;
    // 统计当前聚簇下的，本轮次时间窗口内的hit总数
    private transient ValueState<Double> hitNumberState;
    // hit ratio已经有多少轮次几乎没有发生改变(判断收敛的依据)
    private transient ValueState<Integer> immutabilityCountState;
    // 已经有多少轮次query number 一直为0(判断此cluster是否被遗弃的依据)
    private transient ValueState<Integer> abandonedCountState;
    // 上一轮次的hit ratio
    private transient ValueState<Double> priorHitRatioState;

    // --- Timer
    // 是否已经开启计时器的标志位
    private transient ValueState<Boolean> firstState;
    // 检验一个cluster是否合格的统计量(必须要有一定数量的query进入这个cluster才算做合格，不然直接开启计时器或将导致存在几百个计时器，大量耗费资源)
    private transient ValueState<Integer> qualifiedState;

    public CacheAndLandmark(int winnersMaxSize, int convergence, double negligible, int abandon,
                            int qualified, int hotCluster) {
        this.winnersMaxSize = winnersMaxSize;
        this.convergence = convergence;
        this.negligible = negligible;
        this.abandon = abandon;
        this.qualified = qualified;
        this.hotCluster = hotCluster;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 完全匹配所需的两个基础数据结构
        MapStateDescriptor<Integer, Path> winnersPathStateDescriptor = new MapStateDescriptor<>("winnersPath",
                Types.INT, TypeInformation.of(Path.class));
        MapStateDescriptor<String, ArrayList<Tuple2<Integer, Integer>>> winnersVertexStateDescriptor = new MapStateDescriptor<>("winnersVertex",
                TypeInformation.of(String.class), TypeInformation.of(new TypeHint<ArrayList<Tuple2<Integer, Integer>>>() {
        }));

        // partial hit
        ValueStateDescriptor<RadixTree> radixTreeStateDescriptor = new ValueStateDescriptor<>("radixTree",
                TypeInformation.of(RadixTree.class), new RadixTree());

        // cache size
        ValueStateDescriptor<Integer> winnersSizeStateDescriptor = new ValueStateDescriptor<>("winnersSize", Types.INT, 0);
        ValueStateDescriptor<Integer> hotClusterSizeStateDescriptor = new ValueStateDescriptor<>("hotClusterSize", Types.INT, winnersMaxSize);
        ValueStateDescriptor<Integer> IDSupplierStateDescriptor = new ValueStateDescriptor<Integer>("caAndLaIDSupplier", Types.INT, 1);

        // landmark
        MapStateDescriptor<String, Double> sourceLandmarkStateDescriptor = new MapStateDescriptor<>("sourceLandmark",
                TypeInformation.of(String.class), Types.DOUBLE);
        MapStateDescriptor<String, Double> targetLandmarkStateDescriptor = new MapStateDescriptor<>("targetLandmark",
                TypeInformation.of(String.class), Types.DOUBLE);
        ValueStateDescriptor<Query> ownerClusterStateDescriptor = new ValueStateDescriptor<>("ownerCluster",
                Query.class);

        // hit ratio & abandoned
        ValueStateDescriptor<Integer> queryNumberStateDescriptor = new ValueStateDescriptor<>("queryNumber", Types.INT, 0);
        ValueStateDescriptor<Double> hitNumberStateDescriptor = new ValueStateDescriptor<>("hitNumber", Types.DOUBLE, 0.0);
        ValueStateDescriptor<Integer> immutabilityCountStateDescriptor = new ValueStateDescriptor<>("immutabilityCount", Types.INT, 0);
        ValueStateDescriptor<Integer> abandonedCountStateDescriptor = new ValueStateDescriptor<>("abandonedCount", Types.INT, 0);
        ValueStateDescriptor<Double> priorHitRatioStateDescriptor = new ValueStateDescriptor<>("priorHitRatio", Types.DOUBLE, 0.0);

        // Timer
        ValueStateDescriptor<Boolean> firstStateDescriptor = new ValueStateDescriptor<>("caAndLaFirst", Types.BOOLEAN, true);
        ValueStateDescriptor<Integer> qualifiedStateDescriptor = new ValueStateDescriptor<>("qualified", Types.INT, 0);

        winnersPathState = getRuntimeContext().getMapState(winnersPathStateDescriptor);
        winnersVertexState = getRuntimeContext().getMapState(winnersVertexStateDescriptor);
        radixTreeState = getRuntimeContext().getState(radixTreeStateDescriptor);
        winnersSizeState = getRuntimeContext().getState(winnersSizeStateDescriptor);
        hotClusterSizeState = getRuntimeContext().getState(hotClusterSizeStateDescriptor);
        IDSupplierState = getRuntimeContext().getState(IDSupplierStateDescriptor);
        sourceLandmarkState = getRuntimeContext().getMapState(sourceLandmarkStateDescriptor);
        targetLandmarkState = getRuntimeContext().getMapState(targetLandmarkStateDescriptor);
        ownerClusterState = getRuntimeContext().getState(ownerClusterStateDescriptor);
        queryNumberState = getRuntimeContext().getState(queryNumberStateDescriptor);
        hitNumberState = getRuntimeContext().getState(hitNumberStateDescriptor);
        immutabilityCountState = getRuntimeContext().getState(immutabilityCountStateDescriptor);
        abandonedCountState = getRuntimeContext().getState(abandonedCountStateDescriptor);
        priorHitRatioState = getRuntimeContext().getState(priorHitRatioStateDescriptor);
        firstState = getRuntimeContext().getState(firstStateDescriptor);
        qualifiedState = getRuntimeContext().getState(qualifiedStateDescriptor);
    }

    @Override
    public void processElement(Query query, Context context, Collector<String> collector) throws Exception {
        // 判断是否已经注册计时器
        if (firstState.value()) {
            // 真正的这个clusterID下的第一次请求
            if (qualifiedState.value() == 0) {
                // 存储开创cluster的代表query的相关信息
                ownerClusterState.update(query);
                qualifiedState.update(qualifiedState.value() + 1);
            // 小于10次query说明现在的cluster还没合格呢，很可能马上会被淘汰，不着急注册计时器(目的是防止注册大量无用计时器)
            } else if (qualifiedState.value() < qualified) {
                qualifiedState.update(qualifiedState.value() + 1);
            // cluster合格了，说明可能是一个需要使用的cluster而不是马上被淘汰的那种
            } else {
                logger.info(context.getCurrentKey() + "cluster注册计时器");
                firstState.update(false);
                long timer = context.timerService().currentProcessingTime();
                long timeOffset = random.nextInt(6 * 1000) - 3000;
                context.timerService().registerProcessingTimeTimer(timer + ONE_MINUTE + timeOffset);
            }
        } else {
            // 请求计数加一(只有已经注册过计时器的才可以统计queryNumber)
            int queryNumber = queryNumberState.value();
            queryNumberState.update(queryNumber + 1);
        }

        // ---核心计算部分(最后输出结果是：节点ID的序列字符串)
        // ---cache匹配阶段，先查看是否可以完美或完全匹配，再查询是否可以部分匹配
        // 三个参数的含义(pathID, 在路径序列中的起始位置, 在路径序列中的终止位置)
        Tuple3<Integer, Integer, Integer> matchResult = Tuple3.of(0, 0, 0);
        // 最终结果储存处
        List<String> pathSequence;
//        long startTime = System.nanoTime();
        // ALLT算法的路径结果才可以加入缓存
        boolean addible = false;
        // 进行完美或完全匹配
        pathSequence = checkAllMatch(query ,matchResult);
        // 如果没有匹配到则此对象会是null，所以再进行部分匹配
        if (pathSequence == null) {
            // 进行部分匹配
            pathSequence = checkPartialMatch(query, matchResult);
            // 如果没有匹配到则此对象依旧是null，所以再进行ALLT算法或是A*
            if (pathSequence == null) {
                // 只有注册了计时器，才可以使用ALLT算法，不然直接生成landmark以及cache会永远无法消除，但是很可能又是没用的
                if (firstState.value()) {
                    PathCalculator pathCalculator = new PathCalculator();
                    pathSequence = pathCalculator.getAstarShortestPath(query);
                } else {
                    pathSequence = ALLT(query, sourceLandmarkState, targetLandmarkState);
                    addible = true;
                }
            }
        }

        // 平均执行时间监控部分
//        if (ScheduledStats.ready && pathSequence.size() != 0) {
////            System.out.println("start:" + query.sourceID + "  end:" + query.targetID +
////                    "  time:" + (System.nanoTime() - startTime) / 1000000);
//            ScheduledStats.addTimeUsage(context.getCurrentKey(), System.nanoTime() - startTime);
//        }

        // 可以先把结果输出了，不需要等待缓存添加完成
        collector.collect(pathSequence.toString());
        // --- 核心计算部分结束

        // --- 缓存添加部分
        // 如果query可以被添加进缓存，并且在现有缓存集中匹配不到,进行缓存添加操作
        // (可被添加进缓存的query是>0.5 * cluster.boundEllipse.constant )
        if (query.cacheable && addible && pathSequence.size() != 0) {
            // 未收敛的情况下再进行后续操作
            if (immutabilityCountState.value() < convergence) {
                int winnersSize = winnersSizeState.value();
                if (winnersSize < hotClusterSizeState.value()) {
                    int pathID = IDSupplierState.value();
                    Path path = new Path(pathID, pathSequence, 0);
                    // 加入当前缓存集
                    addCache(winnersPathState, winnersVertexState, radixTreeState, path);
                    IDSupplierState.update(pathID + 1);
                    winnersSizeState.update(winnersSize + 1);
                }
            }
        }
        // --- 缓存添加部分结束
    }

    /**
    计时器主要负责的任务是：1、缓存的更新 2、统计当前时间窗口的请求量，缓存命中率 3、判断此cluster是否收敛或是是否被废弃
    1、更新策略：淘汰缓存中命中频率最低的(LFU-AGING)
    2、请求量是为了观察当前聚簇是否为热点聚簇从而对缓存容量进行调整；hit ratio主要是展示cache的收益
    3-1、如果hit ratio在一段时间内都只有比较小的变化，说明cache已经构建到一个比较优的地步了，可以停止计时器(即不进行无意义的update)
    3-2、如果queryNumber一直为零，说明这个cluster已经被前一个算子融合掉了或者是因为模糊语义被大致包含在另一个cluster中，使得query始终不会发送过来
    (表明此cluster被废弃，可以停止计时器，因为再定时在此cluster上计算也没有意义了)
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // --- 热点聚簇判断与缓存容量更新
        // 如果一个cluster经常被访问，是热点区域的话，我们适当扩大这个cluster下能容纳的cache大小
        if (queryNumberState.value() > hotCluster / 2 && hotClusterSizeState.value() == winnersMaxSize) {
            hotClusterSizeState.update(winnersMaxSize * 2);
        } else if (queryNumberState.value() > hotCluster && hotClusterSizeState.value() == winnersMaxSize * 2) {
            hotClusterSizeState.update(winnersMaxSize * 3);
        }
        // --- 热点聚簇判断与缓存容量更新结束

        logger.info(ctx.getCurrentKey() + "的query number:" + queryNumberState.value() + "; winnersSize:" + winnersSizeState.value());
        // --- 缓存更新(LFU-AGING)
        // cache起码要拥有一些数据再进行update计算
        if (!winnersPathState.isEmpty()) {
            Path outPath = new Path(-1);
            int outMinCount = Integer.MAX_VALUE;
            double outMinLength2 = Double.POSITIVE_INFINITY;

            // 计算缓存里命中频率最低的出列(若命中频率一直比如都是0，选择长度最小的cache出列)
            for (Map.Entry<Integer, Path> entry : winnersPathState.entries()) {
                Path tempPath = entry.getValue();
//                logger.info(tempPath.toString());
                // 每过一个时间窗口，计数变成一半，即防止计数器超限，又能让时间越早的缓存所占的权重越小
                tempPath.count = tempPath.count / 2;
                if (tempPath.count < outMinCount) {
                    outMinCount = tempPath.count;
                    outPath = tempPath;
                } else if (tempPath.count == outMinCount) {
                    if (tempPath.length2 < outMinLength2) {
                        outMinLength2 = tempPath.length2;
                        outPath = tempPath;
                    }
                }
            }

            // 移除缓存中关于此path的缓存(三个数据结构中全部都需要删除)
            int pathID = outPath.pathID;
            winnersPathState.remove(pathID);
            // 这样写虽然会导致一些计算上的浪费，但是代码最为简洁
            for (String dataIndex : outPath.sequence) {
                winnersVertexState.get(dataIndex).removeIf(tuple2 -> tuple2.f0 == pathID);
            }

            RadixTree radixTree = radixTreeState.value();
            radixTree.delete(outPath);
            winnersSizeState.update(winnersSizeState.value() - 1);
        }
        // --- 缓存更新(LFU-AGING)结束

        // --- 判断当前聚簇是否已经收敛或者是被废弃
        // 或根据query number判断是否被遗弃以停止计时器注册
        int queryNumber = queryNumberState.value();
        // 根据queryNumber判断是否遗弃，同时也保证hitRatio不会算出NAN
        if (queryNumber == 0) {
            abandonedCountState.update(abandonedCountState.value() + 1);
        } else {
            // 根据hit ratio判断是否收敛以停止计时器注册
            double hitRatio = hitNumberState.value() / queryNumber;
            ScheduledStats.addQueryNumber(ctx.getCurrentKey(), queryNumber);
            ScheduledStats.addHitNumber(ctx.getCurrentKey(), hitNumberState.value());
            // 将本时间窗口的query number、hit number重置为零
            queryNumberState.update(0);
            hitNumberState.update(0.0);
            // 判断是否收敛(加一个hit ratio > 0的判断的目的在于，如果cache一直没有命中，hit ratio自然没有变化，但这是不算收敛的)
//            if (hitRatio > 0) {
//                if (Math.abs(hitRatio - priorHitRatioState.value()) < negligible) {
//                    immutabilityCountState.update(immutabilityCountState.value() + 1);
//                } else {
//                    immutabilityCountState.update(0);
//                }
//                priorHitRatioState.update(hitRatio);
//            }
        }
        // --- 判断当前聚簇是否已经收敛或者是被废弃

        // --- 收敛或遗弃的后续处理
        // 如果cluster被遗弃了，所有状态无需再保存，清除当前key下所有缓存状态，重置当前key下所有判断状态，以减少内存使用
        // 但是ownerClusterState需要保存，以免cluster被误判遗弃，这样还保留有东山再起的可能
        if (abandonedCountState.value() >= abandon) {
            logger.info(ctx.getCurrentKey() + "已被遗弃");
            // 清除缓存状态
            winnersPathState.clear();
            winnersVertexState.clear();
            radixTreeState.update(new RadixTree());
            sourceLandmarkState.clear();
            targetLandmarkState.clear();
            // 重置判断状态
            winnersSizeState.update(0);
            hotClusterSizeState.update(winnersMaxSize);
            IDSupplierState.update(1);
            queryNumberState.update(0);
            hitNumberState.update(0.0);
            immutabilityCountState.update(0);
            abandonedCountState.update(0);
            priorHitRatioState.update(0.0);
            firstState.update(true);
            qualifiedState.update(1);
            // 当cluster既未收敛又未遗弃时，再注册下一个时间窗口的计时器
        } else if (immutabilityCountState.value() < convergence) {
            // 加一个时间小偏差，防止大量的计时器在某一个时刻集中运行(在程序刚开始运行时会有大量计时器注册)
            long timeOffset = random.nextInt(6 * 1000) - 3000;
            ctx.timerService().registerProcessingTimeTimer(timestamp + ONE_MINUTE + timeOffset);
        }
        // --- 收敛或遗弃的后续处理
    }

    @Override
    public void close() throws Exception {

    }

    private List<String> checkAllMatch(Query query, Tuple3<Integer, Integer, Integer> matchResult) throws Exception {
        // 进行完美或完全匹配
        if (cacheAllMatch(winnersVertexState, query, matchResult)) {
            return getSubPath(winnersPathState, matchResult);
        }
        // 如果没有完美匹配或完全匹配则返回空对象
        return null;
    }

    private List<String> checkPartialMatch(Query query, Tuple3<Integer, Integer, Integer> matchResult) throws Exception {
        // 进行部分匹配
        if (cachePartialMatch(radixTreeState, query, matchResult)) {
            return getPartialHitPath(winnersPathState, query, matchResult);
        }
        // 如果没有部分匹配则返回空对象
        return null;
    }

    /**
    从inverted map(即winnersVertexState或candidatesVertexState)查找是否有匹配的cache
    query为当前需要匹配的请求
    matchResult存储匹配结果
     */
    private boolean cacheAllMatch(MapState<String, ArrayList<Tuple2<Integer, Integer>>> invertedMapState, Query query,
                                  Tuple3<Integer, Integer, Integer> matchResult) throws Exception {
        // 如果有一个节点不在倒排索引中，直接说明缓存没有perfect hit以及 complete hit，直接采取partial hit计算
        if (invertedMapState.contains(query.sourceID) && invertedMapState.contains(query.targetID)) {
            ArrayList<Tuple2<Integer, Integer>> sourceTable = invertedMapState.get(query.sourceID);
            ArrayList<Tuple2<Integer, Integer>> targetTable = invertedMapState.get(query.targetID);
            // 生成cache匹配结果
            for (Tuple2<Integer, Integer> sourceTuple2 : sourceTable) {
                for (Tuple2<Integer, Integer> targetTuple2 : targetTable) {
//                    logger.info(targetTuple2.toString());
                    if (Objects.equals(targetTuple2.f0, sourceTuple2.f0)) {
                        logger.info("full hit");
                        matchResult.setFields(targetTuple2.f0, sourceTuple2.f1, targetTuple2.f1);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
    查找是否有部分缓存的匹配
    思路是所有缓存路径的点存储在radix tree中，然后在radix tree中查找起终点对应格网是否有相同缓存路径ID的点
     */
    private boolean cachePartialMatch(ValueState<RadixTree> radixTreeState,
            Query query, Tuple3<Integer, Integer, Integer> matchResult) throws Exception {
        RadixTree radixTree = radixTreeState.value();
        S2CellId sourceCellId = S2CellId.fromPoint(query.source);
        TreeNode sourceNode = radixTree.getByS2CellId(sourceCellId);
        if (sourceNode == null) {
            return false;
        }

        S2CellId targetCellId = S2CellId.fromPoint(query.target);
        TreeNode targetNode = radixTree.getByS2CellId(targetCellId);
        if (targetNode == null) {
            return false;
        }

        List<Integer> pathIDS = sourceNode.getPathID();
        List<Integer> pathIDT = targetNode.getPathID();
        for (int i = 0 ; i < pathIDS.size() ; ++i) {
            for (int j = 0 ; j < pathIDT.size() ; ++j) {
                if (Objects.equals(pathIDS.get(i), pathIDT.get(j))) {
                    logger.info("partial hit");
                    matchResult.setFields(pathIDS.get(i), sourceNode.getLeafValue().get(i), targetNode.getLeafValue().get(j));
                    return true;
                }
            }
        }
        return false;
    }

    /**
   获得部分缓存命中下的路径
    */
    private List<String> getPartialHitPath(MapState<Integer, Path> pathMapState,
                                           Query query, Tuple3<Integer, Integer, Integer> matchResult) throws Exception {
        List<String> pathSequence;
        PathCalculator pathCalculator = new PathCalculator(32);
        // 获得部分命中的head 以及 tail，并生成相应的query
        Path path = pathMapState.get(matchResult.f0);
        String head = path.sequence.get(matchResult.f1);
        String tail = path.sequence.get(matchResult.f2);
        // 由于返回的路径是倒序的，为了路径顺序正确，我们需要将起终点对调生成query
        Query queryS = new Query(head, query.sourceID, TopologyGraph.getVertex(head), query.source);
        Query queryT = new Query(query.targetID, tail, query.target, TopologyGraph.getVertex(tail));
        // 将三段结果路径进行拼凑
        pathSequence = pathCalculator.getAstarShortestPath(queryS);
        pathSequence.addAll(getSubPath(pathMapState, matchResult));
        List<String> tailSequence = pathCalculator.getAstarShortestPath(queryT);
        if (tailSequence.size() != 0) {
            tailSequence.remove(0);
        }
        pathSequence.addAll(tailSequence);
        return pathSequence;
    }

    /**
    利用匹配结果，获取对应路径下的子序列
     */
    private List<String> getSubPath(MapState<Integer, Path> pathMapState,
                                    Tuple3<Integer, Integer, Integer> matchResult) throws Exception {
//        logger.info("-----cache hit-----");
        // 获得匹配到的缓存路径并把hit number计数器加一
        Path path = pathMapState.get(matchResult.f0);
        // 未收敛的情况下持续统计hit number(收敛以后 hit ratio基本不变再统计也缺少意义)
        // 另外由于onTimer不再执行，如果这里再持续增加，会导致count超限(所以收敛后不再执行)
        if (immutabilityCountState.value() < convergence) {
            path.count();
            hitNumberState.update(hitNumberState.value() + 1);
        }
        List<String> subSequence;
        // fromIndex必须满足小于toIndex，所以需要加一步判断
        // fromIndex inclusive ; toIndex exclusive 所以toIndex需要+1
        if (matchResult.f1 > matchResult.f2) {
            // subList返回的是原有对象的部分引用，为了不修改原对象，我们需要新建
            subSequence = new ArrayList<>(path.getSequence().subList(matchResult.f2, matchResult.f1 + 1));
            // 现在的顺序是倒的，我们还需要反转顺序
            Collections.reverse(subSequence);
        } else {
            subSequence = path.getSequence().subList(matchResult.f1, matchResult.f2 + 1);
        }
        return subSequence;
    }

    /**
    ALLT算法的具体逻辑
    相关具体实现集中在PathCalculator中
    逻辑为，当前聚簇的第一个请求执行麻烦但搜索空间大的计算，以生成landmark(我们选择A*，Dijkstra的搜索空间过于大了)
    在可能的情况下，尽量使用local landmark(并在内部也尽一切可能使用landmark来生成lower bound)，如果没法使用，回退到A*逃生
     */
    private List<String> ALLT(Query query, MapState<String, Double> sourceLandmarkState,
                              MapState<String, Double> targetLandmarkState) throws Exception {
        PathCalculator pathCalculator = new PathCalculator();
        // landmarkState为空，说明是当前cluster合格以后的第一个query
        // 需要对representative query执行更耗费时间的Dijkstra计算生成更全面的local landmark并赋值
        if (sourceLandmarkState.isEmpty()) {
            // 获得representative query生成local landmark
            Query representativeQuery = ownerClusterState.value();
            // 计算得到结果
            pathCalculator.getAstarShortestPath(representativeQuery);
            HashMap<String, Double> sourceLandmarkHashMap = pathCalculator.getCloseMap();
            // representative query的closeMap刚好是作为我们的landmark
            // 记得先putAll再算下一个landmark，不然closeMap会被清空
            sourceLandmarkState.putAll(sourceLandmarkHashMap);
            // 获得相反方向的representativeQuery
            Query oppositeQuery = representativeQuery.getOppositeQuery();
            pathCalculator.getAstarShortestPath(oppositeQuery);
            HashMap<String, Double> targetLandmarkHashMap = pathCalculator.getCloseMap();
            targetLandmarkState.putAll(targetLandmarkHashMap);
        }

        // 由于landmark中不一定拥有当前query起终点的距离数据，故还需要有逻辑做具体调用的函数的判断逻辑
        // 即尽可能利用两个landmark,有部分缺失尝试能否利用其一，最后用A*逃生
        return getLandmarkShortestPath(query, sourceLandmarkState, targetLandmarkState, pathCalculator);
    }

    /**
    进一步判断能否两个landmark都利用上,还是回退到A*逃生
    (由于S到T和T到S实际上是等价的，都包含的情况下选择哪一个都一样
    所以我们主要是考虑，landmark只包含了其中一个点的情况，选择包含的点作为实际的起始搜索点)
     */
    private List<String> getLandmarkShortestPath(Query query, MapState<String, Double> sourceLandmarkState,
                                                 MapState<String, Double> targetLandmarkState,
                                                 PathCalculator pathCalculator) throws Exception {
        List<String> resultList;
        boolean isTargetInSL = sourceLandmarkState.contains(query.targetID);
        boolean isTargetInTL = targetLandmarkState.contains(query.targetID);
        boolean isSourceInSL = sourceLandmarkState.contains(query.sourceID);
        boolean isSourceInTL = targetLandmarkState.contains(query.sourceID);
        // 如果landmark中都包含query终点或者都包含query起点，那么我们使用两个landmark执行ALT算法
        // 利用landmark生成tighter lower bound 执行更快速的local landmark计算
        // 大部分无法命中缓存的query都是在这里和下一个判断分支这两个分支流中实际解决的
        if (isTargetInSL && isTargetInTL) {
            resultList = pathCalculator.getLandmarkShortestPath(query, sourceLandmarkState, targetLandmarkState);
//            ScheduledStats.addCloseMapSize(pathCalculator.getCloseMap().size(), 2);
//            pathCalculator.getLandmarkShortestPath(query, sourceLandmarkState);
//            int num = pathCalculator.getCloseMap().size();
//            pathCalculator.getLandmarkShortestPath(query, targetLandmarkState);
//            ScheduledStats.addCloseMapSize((pathCalculator.getCloseMap().size() + num) / 2, 1);
//            pathCalculator.getAstarShortestPath(query);
//            ScheduledStats.addCloseMapSize(pathCalculator.getCloseMap().size(), 0);
        } else if (isSourceInSL && isSourceInTL) {
            resultList = pathCalculator.getLandmarkShortestPath(query.getOppositeQuery(), sourceLandmarkState, targetLandmarkState);
        } else if (isTargetInSL){
            // 此判断解决的问题是如果起终点只有一个包含在landmark中怎么办以及我们选择哪一个点作为实际计算的终点，即确定搜索方向
            // 由于source target是等价的，所以我们默认从sourceLandmark开始判断，先后顺序并没有特殊意义
            resultList = pathCalculator.getLandmarkShortestPath(query, sourceLandmarkState);
        } else if (isTargetInTL) {
            resultList = pathCalculator.getLandmarkShortestPath(query, targetLandmarkState);
        } else if (isSourceInSL) {
            resultList = pathCalculator.getLandmarkShortestPath(query.getOppositeQuery(), sourceLandmarkState);
        } else if (isSourceInTL) {
            resultList = pathCalculator.getLandmarkShortestPath(query.getOppositeQuery(), targetLandmarkState);
        } else {
            // 起终点都不在landmark表中，回退到A*逃生
            resultList = pathCalculator.getAstarShortestPath(query);
        }
        return resultList;
    }

    /*
    想要新加入的缓存路径与当前缓存中所有缓存路径的相似性检查，我们的目的是让存入的缓存尽可能充满当前区域
     */
//    private boolean checkSimilarity(MapState<Integer, Path> pathState, Path newPath) throws Exception {
//        S2Cell s2Cell = S2Cell.fromFacePosLevel(0, 0, ParametersPasser.granularity + 4);
//        double minDistance = s2Cell.getVertex(0).getDistance2(s2Cell.getVertex(1));
//        // 对当前缓存路径中的每一条进行距离检查，如果两条路径相交，则直接返回0小于minDistance
//        for (Path path : pathState.values()) {
//            if (newPath.getFirstVertex().getDistance2(path.getFirstVertex()) < minDistance &&
//            newPath.getLastVertex().getDistance2(path.getLastVertex()) < minDistance) {
//                logger.info("相似缓存");
//                return false;
//            }
//            if (newPath.getFirstVertex().getDistance2(path.getLastVertex()) < minDistance &&
//            newPath.getLastVertex().getDistance2(path.getFirstVertex()) < minDistance) {
//                logger.info("相似缓存");
//                return false;
//            }
//        }
//        return true;
//    }

    /**
    提供把路径序列添加进缓存的功能(需要添加进三个结构中path array ,inverted list, radix tree)
     */
    private void addCache(MapState<Integer, Path> pathState, MapState<String, ArrayList<Tuple2<Integer, Integer>>> vertexState,
                          ValueState<RadixTree> radixTreeState, Path path) throws Exception {
        int pathID = path.pathID;
        List<String> pathSequence = path.sequence;
        if (pathState != null) {
            // 计算此缓存路径的距离，在缓存更新阶段中，优先排除length2更小的cache
            double length2 = TopologyGraph.getDistance2(TopologyGraph.getVertex(pathSequence.get(0)),
                    TopologyGraph.getVertex(pathSequence.get(pathSequence.size() - 1)));
            // 添加进Path表
            path.setLength2(length2);
            pathState.put(pathID, path);
        }

        // 添加进Vertex表(inverted map)
        int sequencePos = 0;
        for (String dataIndex : pathSequence) {
            if (vertexState.contains(dataIndex)) {
                vertexState.get(dataIndex).add(Tuple2.of(pathID, sequencePos));
            } else {
                // invertedList不需要太长，因为感觉每个节点不会有太多的cache路径经过(节省内存)
                ArrayList<Tuple2<Integer, Integer>> invertedList = new ArrayList<>(3);
                invertedList.add(Tuple2.of(pathID, sequencePos));
                vertexState.put(dataIndex, invertedList);
            }
            ++sequencePos;
        }

        // radix tree嵌入缓存的具体逻辑已经被封装到insert函数中
        RadixTree radixTree = radixTreeState.value();
        radixTree.insert(path);
        radixTreeState.update(radixTree);
    }

    /*
    根据landmark behind query的程度，选择更适合的landmark
    此函数以及下面被调用的函数被废弃，因为我们经过实验发现，通过角度判断适合landmark是错误的以及通过距离判断也是不对的
    故我们现在的思路是尽最大可能利用起两个landmark以最小化搜索空间，已经不存在有选择更合适的landmark这层逻辑
     */
    @Deprecated
    private boolean chooseLandmark(Query query, ValueState<Query> ownerClusterState) throws Exception {
        // 形成四个点的墨卡托投影经纬度
        S2LatLng s2LatLngS = new S2LatLng(ownerClusterState.value().source);
        S2LatLng s2LatLngT = new S2LatLng(ownerClusterState.value().target);
        S2LatLng s2LatLngQueryS = new S2LatLng(query.source);
        S2LatLng s2LatLngQueryT = new S2LatLng(query.target);

        // 计算向量值
        S2LatLng sToQueryS = s2LatLngS.sub(s2LatLngQueryS);
        S2LatLng sToQueryT = s2LatLngS.sub(s2LatLngQueryT);
        S2LatLng tToQueryS = s2LatLngT.sub(s2LatLngQueryS);
        S2LatLng tToQueryT = s2LatLngT.sub(s2LatLngQueryT);

        // 计算cluster.s还是cluster.t更和query S T更近似一条直线
        double sCosQuery = getAbsCos(sToQueryS, sToQueryT);
        double tCosQuery = getAbsCos(tToQueryS, tToQueryT);
        return sCosQuery > tCosQuery;
    }

    /*
    获取角度cos的绝对值
     */
    @Deprecated
    private double getAbsCos(S2LatLng s2LatLngS, S2LatLng s2LatLngT) {
        double dotProd = s2LatLngS.lngRadians() * s2LatLngT.lngRadians() + s2LatLngS.latRadians() * s2LatLngT.latRadians();
        double normS = Math.sqrt(Math.pow(s2LatLngS.lngRadians(), 2) + Math.pow(s2LatLngS.latRadians(), 2));
        double normT = Math.sqrt(Math.pow(s2LatLngT.lngRadians(), 2) + Math.pow(s2LatLngT.latRadians(), 2));
        return Math.abs(dotProd / (normS * normT));
    }
}
