package flinkdemo;

public class ToDoList {
    /**
     * 1、QueryCluster阶段，通过可视化发现某些聚簇的重叠部分有些多，寻找算法让聚簇变得分散一些，即使有少部分区域无覆盖也无所谓
     * 这一条不好写，但不影响其余部分，先推后处理
     * 2、部分缓存匹配阶段，把原来的PointIndex替换为我们新写的RadixTree
     * 3、LandMark生成可以替换为A*，并且实验验证下，确认是否需要生成两个Landmark
     * 已解决，我们需要生成两个landmark并在pathCalculator中判断使用那个landmark的heuristics，通过角度事先判断或者通过距离事先判断都可能会得到错误结果
     * 这样子我们通过实验发现起码可以节省50%的搜索空间大小,并且现已经有A*替代Dijkstra生成landmark
     * 4、请求生成方面，用一个POI集合作为起终点(感觉上可以增加缓存命中率，但也不一定；把1、2、3修改后，内存使用应该是明显下降的，则可以考虑增加缓存路径的条数)
     */
}
