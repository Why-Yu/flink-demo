package flinkdemo.operator;

import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2RegionCoverer;
import flinkdemo.entity.Cluster;
import flinkdemo.entity.Query;
import flinkdemo.util.ParametersPasser;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*
!!!功能：对query进行搜索空间预估并把相关度最高的query都划分到同一个cluster中去
提高后续localCache的命中率以及landmark的使用次数
 */
public class QueryCluster extends KeyedProcessFunction<Integer, Query, Query> {
    public static final Logger logger = LoggerFactory.getLogger(QueryCluster.class);
    // 注册的计时器时间窗口
    private static final long TEN_SECONDS = 10 * 1000;
    // 利用随机数制造时间小偏差，防止计时器的拥挤
    private static final Random random = new Random();
    // 多少次计算之后cluster的数量不再发生，则判断收敛，不再注册计时器(因为我们估计，代码运行一段时间后，cluster必将收敛)
    private final int convergence;
    // 估计搜索空间的离心率(近似椭圆)
    private final double eccentricity;
    //存储当前key下，所有聚簇
    private transient ListState<Cluster> clusterListState;
    // 用于快速判断cluster是否已经做过融合计算(因为我们在实验过程中发现，融合计算是整个算法的性能瓶颈)
    // 所以我们不再机械的逐一检查cluster列表中后一半能否融合进前一半(注意这个过程实际上绝大部分实在重复计算，因为之前轮次其实算过了)
    // 在这种情况我们的优化是，对这一轮次新加入的cluster，查看比它小的已有cluster是否可以融合进来
    // 因为我们已知新加入的聚簇不可能被包含在已有聚簇中(否则queryInCluster函数就会命中)，所以只会出现新加入的cluster可能包含已有cluster的情况出现
    private transient ListState<String> newClusterListState;
    // 统计已经有多少轮次clusterList没有发生任何改变
    private transient ValueState<Integer> timeCountState;
    // ID提供器
    private transient ValueState<Integer> IDSupplierState;
    // 判断是否需要第一次注册计时器开启收敛计算
    private transient ValueState<Boolean> firstState;

    public QueryCluster(int convergence, double eccentricity) {
        this.convergence = convergence;
        this.eccentricity = eccentricity;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Cluster> listStateDescriptor = new ListStateDescriptor<>("clusterState", Cluster.class);
        ListStateDescriptor<String> newClusterListStateDescriptor = new ListStateDescriptor<>("newClusterList", String.class);
        ValueStateDescriptor<Integer> timeCountStateDescriptor = new ValueStateDescriptor<>("timeCount", Types.INT, 0);
        ValueStateDescriptor<Integer> IDSupplierStateDescriptor = new ValueStateDescriptor<>("clusterIDSupplier", Types.INT, 1);
        ValueStateDescriptor<Boolean> firstStateDescriptor = new ValueStateDescriptor<>("queryFirst", Types.BOOLEAN, true);
        clusterListState = getRuntimeContext().getListState(listStateDescriptor);
        newClusterListState = getRuntimeContext().getListState(newClusterListStateDescriptor);
        timeCountState = getRuntimeContext().getState(timeCountStateDescriptor);
        IDSupplierState = getRuntimeContext().getState(IDSupplierStateDescriptor);
        firstState = getRuntimeContext().getState(firstStateDescriptor);
    }

    @Override
    public void processElement(Query query, Context context, Collector<Query> collector) throws Exception {
        if (firstState.value()) {
            firstState.update(false);
            logger.info(context.getCurrentKey() + "方向窗口第一次注册计时器");
            long timer = context.timerService().currentProcessingTime();
            long timeOffset = random.nextInt(1000) - 500;
            context.timerService().registerProcessingTimeTimer(timer + TEN_SECONDS + timeOffset);
        }

        // 遍历query是否落在某个cluster中，若有，则附上clusterID,下一个算子依据特定clusterID进行逻辑分区
        // 每一次onTimer都对clusterList排序，按照cluster.radius由大到小，所以这里也是有顺序的，先对顺序的原有聚簇从大到小查看是否IN
        // 如果没有匹配上，再对这一轮次中新添加的cluster再匹配
        // 获取自适应粒度
        int granularity = ParametersPasser.getGranularity();
        // 生成相关粒度的cover计算器(因为匹配计算可以复用此计算器，所以在方法外部生成，再传递引用)
        S2RegionCoverer s2RegionCoverer = S2RegionCoverer.builder().setMaxLevel(granularity).build();
        for(Cluster cluster : clusterListState.get()) {
            if (queryInCluster(query, cluster, s2RegionCoverer)) {
                query.setClusterID(cluster.clusterID);
                // 如果query长度大于所属聚簇椭圆的a，则设为可缓存的
                if (query.getDistance() > 0.5 * cluster.boundEllipse.constant) {
                    query.setCacheable(true);
                }
                collector.collect(query);
                break;
            }
        }
        // clusterID还是初始值,说明在上一步根本没有匹配到
        if (query.clusterID.equals("NoMatched")) {
            int countID = IDSupplierState.value();
            // 依据方向窗口标识符和此窗口下的IDSupplier组合成为cluster的全局唯一ID
            String clusterID = context.getCurrentKey() + "-" + countID;
            Cluster cluster = new Cluster(clusterID, query.getSource(), query.getTarget(), eccentricity);
            clusterListState.add(cluster);
            query.setClusterID(clusterID);
            // 创建此cluster的query总是cacheable
            query.setCacheable(true);
            IDSupplierState.update(countID + 1);
            // 添加进此轮次新加入cluster列表
            newClusterListState.add(clusterID);
            collector.collect(query);
        }
    }

    /*
    融合cluster，尽量减少clusterList的冗余
    判断依据是，若包含，则把小的cluster融合进大的cluster
    融合计算是比较耗费计算资源的，所以我们每隔一段时间融合一次
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Query> out) throws Exception {
        // 准备融合的cluster list
        List<Cluster> mergeList = new ArrayList<>();
        // 记录下需要被融合的元素，其实也就是把对应元素在clusterListState删去
        List<String> removeClusterList = new ArrayList<>();
        for (Cluster cluster : clusterListState.get()) {
            mergeList.add(cluster);
        }
        // 默认按照升序排序，我们这里必须采用降序,list中按椭圆的2a由大到小
        // 降序可以保证，在查询query落在哪一个cluster中时，可以按照cluster从大到小依次遍历
        Collections.sort(mergeList);
//        if (ctx.getCurrentKey() == 3) {
//            for (Cluster cluster : mergeList) {
//                logger.info("constant:" + String.valueOf(cluster.boundEllipse.constant));
//            }
//            logger.info("mergeListSize:" + mergeList.size());
//        }


        // 只需要在cluster首次加入的时候，查看一下比它小的cluster是否能融合进来就是完成了精确的融合计算
        // 在原先的程序中每一次计时器都查看后一半能否融合进前一半的机械思路下(不仅机械，也是不精确的)
        // 经过实验，发现即使是这样，计算量还是太大(在每秒喂16个query的情况下)，每个角度窗口下有很多cluster而contains的计算又很耗费资源
        // 而其实很多融合计算都是重复的，因为之前实际上已经计算过融合情况
        // 所以经过思考，我们增加一个state，用来记录本时间轮次新加入的cluster，因为只有queryInCluster全部返回false,才会加入新cluster
        // 所以新加入的cluster必定不会被已有cluster包含，这样我们只需要检查比新加入的cluster小的已有cluster即可完成精确融合计算
        // (题外话:聚簇不是覆盖越广越好，聚簇更加local一些对于解决落在其内部的query更加有效，但与此带来的问题是大query无法用小聚簇来解决)
        for (String newClusterID : newClusterListState.get()) {  // 检查顺序无所谓先后，新加入的cluster就算有包含关系也可以返回正确的结果
            for (int i = mergeList.size() - 1 ; i >= 0 ; --i) {
                if (mergeList.get(i).clusterID.equals(newClusterID)) {
                    for (int j = mergeList.size() - 1; j > i ; --j) {
                        // 聚簇融合条件是大Cap完全包含了小Cap
                        if (mergeList.get(i).contains(mergeList.get(j))) {
                            // clusterID可以重复添加，这个是无所谓的
                            removeClusterList.add(mergeList.get(j).clusterID);
                        }
                    }
                    break;
                }
            }
        }
        // clear状态，为记录下一轮次做准备
        newClusterListState.clear();

        if (removeClusterList.size() > 0) {
            logger.info(removeClusterList.toString());
            // 直接删除对应元素并对状态进行更新
            mergeList.removeIf(cluster -> removeClusterList.contains(cluster.clusterID));
            clusterListState.update(mergeList);
            timeCountState.update(0);
        } else {
            // 本次没有融合任何cluster,未融合计数器加一
            int timeCount = timeCountState.value();
            timeCountState.update(timeCount + 1);
        }

        //如果还未收敛，则注册下一个计时器
        if (timeCountState.value() < convergence) {
            long timeOffset = random.nextInt(1000) - 500;
            ctx.timerService().registerProcessingTimeTimer(timestamp + TEN_SECONDS + timeOffset);
        } else {
            // 判断结果已收敛
            logger.info(ctx.getCurrentKey() + "分区聚簇已收敛");
            for (Cluster cluster : mergeList) {
                logger.info(cluster.clusterID + "constant:" + String.valueOf(cluster.boundEllipse.constant));
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    /*
    检查query是否落在cluster中
    判断依据是：query的source和target的自适应粒度cell需要和cluster的boundCap有相交
    相比之前单纯判断点落在搜索空间内的改进有：
    1、我们需要模糊的语义而不是精确语义，完全精确会导致搜索空间稍有不同就产生新cluster，导致cluster生成过多，且大部分冗余
    2、自适应粒度的cell，可根据query的平均长度来自动调整cell的大小，使得在模糊语义的情境下，也能保持一定的精度
     */
    private boolean queryInCluster(Query query, Cluster cluster, S2RegionCoverer s2RegionCoverer) {
        ArrayList<S2CellId> cellIdArrayList = new ArrayList<>();
        // 得到source的自适应粒度cell
        s2RegionCoverer.getCovering(query.getSource(), cellIdArrayList);
        S2Cell sourceCell = new S2Cell(cellIdArrayList.get(0));
        // 若与boundCap没有相交，直接可以判断query不落在cluster中，节省后续计算
        if (!cluster.boundEllipse.mayIntersect(sourceCell)) return false;
        // 得到target的自适应粒度cell
        s2RegionCoverer.getCovering(query.getTarget(), cellIdArrayList);
        S2Cell targetCell = new S2Cell(cellIdArrayList.get(0));
        // 两个都相交，返回true，有一个未相交，返回false
        return cluster.boundEllipse.mayIntersect(targetCell);
    }
}
