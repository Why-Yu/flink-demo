package flinkdemo.operator;

import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2RegionCoverer;
import flinkdemo.entity.Cluster;
import flinkdemo.entity.Query;
import flinkdemo.util.ParametersPasser;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
!!!功能：对query进行搜索空间预估并把相关度最高的query都划分到同一个cluster中去
提高后续localCache的命中率以及landmark的使用次数
 */
public class QueryCluster extends KeyedProcessFunction<Integer, Query, Query> {
    public static final Logger logger = LoggerFactory.getLogger(QueryCluster.class);

    private static final long TEN_SECONDS = 10 * 1000;
    // 多少次计算之后cluster的数量不再发生，则判断收敛，不再注册计时器(因为我们估计，代码运行一段时间后，cluster必将收敛)
    private final int convergence;
    // 估计搜索空间的离心率(近似椭圆)
    private final double eccentricity;
    //存储当前key下，所有聚簇
    private transient ListState<Cluster> clusterListState;
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
        ListStateDescriptor<Cluster> listStateDescriptor = new ListStateDescriptor<Cluster>("clusterState", Cluster.class);
        ValueStateDescriptor<Integer> timeCountStateDescriptor = new ValueStateDescriptor<Integer>("timeCount", Types.INT, 0);
        ValueStateDescriptor<Integer> IDSupplierStateDescriptor = new ValueStateDescriptor<Integer>("clusterIDSupplier", Types.INT, 1);
        ValueStateDescriptor<Boolean> firstStateDescriptor = new ValueStateDescriptor<Boolean>("queryFirst", Types.BOOLEAN, true);
        clusterListState = getRuntimeContext().getListState(listStateDescriptor);
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
            context.timerService().registerProcessingTimeTimer(timer + TEN_SECONDS);
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
            IDSupplierState.update(countID + 1);
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
        if (ctx.getCurrentKey() == 3) {
            for (Cluster cluster : mergeList) {
                logger.info("constant:" + String.valueOf(cluster.boundEllipse.constant));
            }
            logger.info("mergeListSize:" + mergeList.size());
        }

        // 查看后一半小的是否能融合进前一半的大聚簇(不用做精确融合，以节省计算资源)
        // 其实感觉聚簇不是覆盖越广越好，聚簇更加local一些对于解决落在其内部的query更加有效，但与此带来的问题是大query无法用小聚簇来解决
        int middle = mergeList.size() / 2;
        for (int i = mergeList.size() - 1 ; i >= middle ; i--) {
            // 优先把小的聚簇融合进尽可能大的聚簇中
            for (int j = 0; j < middle ; j++) {
                // 聚簇融合条件是大Cap完全包含了小Cap
                if (mergeList.get(j).contains(mergeList.get(i))) {
                    removeClusterList.add(mergeList.get(i).clusterID);
                    break;
                }
            }
        }

        if (removeClusterList.size() > 0) {
//            logger.info(removeClusterList.toString());
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
            ctx.timerService().registerProcessingTimeTimer(timestamp + TEN_SECONDS);
        } else { //如果收敛了，最后再做一次精确融合，因为现在只剩下少数大聚簇了，但也有可能存在包含，这样最后再尽可能减少内存使用量
            for (int i = mergeList.size() - 1 ; i >= 1 ; i--) {
                for (int j = i - 1 ; j >= 0 ; j--) {
                    // 聚簇融合条件是大Cap完全包含了小Cap
                    if (mergeList.get(j).contains(mergeList.get(i))) {
                        removeClusterList.add(mergeList.get(i).clusterID);
                        break;
                    }
                }
            }
            mergeList.removeIf(cluster -> removeClusterList.contains(cluster.clusterID));
            clusterListState.update(mergeList);
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
