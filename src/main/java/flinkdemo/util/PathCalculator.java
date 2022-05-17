package flinkdemo.util;

import com.google.common.geometry.S1Angle;
import com.google.common.geometry.S2Point;
import flinkdemo.entity.Node;
import flinkdemo.entity.Query;
import org.apache.flink.api.common.state.MapState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/*
最短路径计算器
 */
public strictfp class PathCalculator {
    public static final Logger logger = LoggerFactory.getLogger(PathCalculator.class);
    // openMap需要存储节点，因为节点的相关信息在计算时是要使用的
    public HashMap<String, Node> openMap;
    // 为节省内存，closeMap无需再存储节点，因为我们的启发函数满足一致性要求，closeMap是不会reopen更新的
    // 也方便直接转化为landmarkState
    public HashMap<String, Double> closeMap;
    // 最小二叉堆，方便我们每次取出花费最小的节点
    public BinaryMinHeap<Node> minHeap;
    // pathCalculator当然可以重复使用，不然怎么叫计算器，所以需要这个变量来判断是否需要刷新openMap;closeMap;minHeap
    private boolean isUsed = false;
    // pathCalculator中Map以及Heap的初始大小
    private final int capacity;

    public PathCalculator() {
        this.capacity = 512;
        this.openMap = new HashMap<>(capacity);
        this.closeMap = new HashMap<>(capacity);
        this.minHeap = new BinaryMinHeap<>(Node.class, capacity / 2);
    }

    public PathCalculator(int capacity) {
        this.capacity = capacity;
        this.openMap = new HashMap<>(capacity);
        this.closeMap = new HashMap<>(capacity);
        this.minHeap = new BinaryMinHeap<>(Node.class, capacity / 2);
    }

    /**
     * query.target就是搜索的终点，需要注意输入的query的方向
     */
    public List<String> getLandmarkShortestPath(Query query, MapState<String, Double> landmarkState) throws Exception {
        // 如果路径计算器已经被使用过，把map刷新一下
        // 但实际上我们现有代码一次query只会调用一次路径计算，并不会重复使用路径计算器(这样写是为了以后扩展时，防止出现意外错误)
        if (isUsed) {
            refresh();
        }
        // ----- 初始化工作
        List<String> resultList = new ArrayList<>(200);
        Node currentNode = new Node(query.sourceID, query.source, 0.0);;
        double viewDistance = landmarkState.get(query.targetID);;
        String goalID = query.targetID;;
        S2Point goal = query.target;;
        closeMap.put(query.sourceID, 0.0);
        // -----
        //---test
//        double total = 0;
//        double better = 0;
        //---
        // -----算法开始
        while (!currentNode.dataIndex.equals(goalID)) {
            for (Node node : TopologyGraph.getUnclosedLinkedNode(currentNode, closeMap)) {
                // openMap中不存在说明是第一次被扩展到，计算启发函数值，加入openMap
                if (!openMap.containsKey(node.dataIndex)) {
                    // landmarkState可能返回为null，因为我们使用的是local landmark。用optional包装
                    Optional<Double> opt = Optional.ofNullable(landmarkState.get(node.dataIndex));
                    // 不为null，计算lower bound，否则使用三维直线距离当作逃生，计算lower bound
                    // Math.abs的本质是Math.max(d(l, a) - d(l, b), d(l, b) - d(l, a))
                    double eDistance = getDistance(node, goal);
                    double heuristics = opt.map(distance -> Math.abs(viewDistance - distance))
                            .orElseGet(() -> eDistance);
                    if (heuristics < eDistance) {
                        heuristics = eDistance;
                    }
                    // --- test begin
//                    total += 1;
//                    double distance = getDistance(node , goal);
//                    if (heuristics > distance) {
//                        logger.info("h:" + heuristics + "   d:" + distance);
//                        better += 1;
//                    }
                    // --- test end
                    node.total = node.gCost + heuristics;
                    node.parent = currentNode;
                    openMap.put(node.dataIndex, node);
                    minHeap.add(node);
                } else {
                    // 如果当前的扩展出去的节点其路径由于之前扩展到此节点的路径，则进行更新
                    if (openMap.get(node.dataIndex).gCost > node.gCost) {
                        Node revisedNode = openMap.get(node.dataIndex);
                        revisedNode.total = revisedNode.total - (revisedNode.gCost - node.gCost);
                        revisedNode.gCost = node.gCost;
                        revisedNode.parent = currentNode;
                        minHeap.swim(revisedNode);
                    }
                }
            }
            // 更新currentNode，closeMap，openMap
            if (!openMap.isEmpty()) {
                currentNode = minHeap.delMin();
                closeMap.put(currentNode.dataIndex, currentNode.gCost);
                openMap.remove(currentNode.dataIndex);
            } else {
                return new ArrayList<>();
            }
        }
        // ----- 算法结束

        isUsed = true;
        // 生成结果路径
        while (currentNode != null) {
            resultList.add(currentNode.dataIndex);
            currentNode = currentNode.parent;
        }
//        logger.info("加速比例:" + String.valueOf(better / total) + "   total:" + total + "   resultSize:" + resultList.size()
//        + "   openMapSize:" + openMap.size() + "   closeMap:" + closeMap.size() + "   length:" +
//                getDistance(new Node("1", query.source, 0.0), query.target));
//        logger.info("landmark:" + String.valueOf(total) + "   query:" + query.sourceID);
//        logger.info("landmark resolve");
        return resultList;
    }

    /**
     * 注意：这里的query.target必须要在landmark表中有值，
     * 所以在调用它的算子中，如果是把source当作target，则需要把要把query的起终点调转
     */
    public List<String> getLandmarkShortestPath(Query query, MapState<String, Double> sourceLandmarkState,
                                                MapState<String, Double> targetLandmarkState) throws Exception {
        // 如果路径计算器已经被使用过，把map刷新一下
        // 但实际上我们现有代码一次query只会调用一次路径计算，并不会重复使用路径计算器(这样写是为了以后扩展时，防止出现意外错误)
        if (isUsed) {
            refresh();
        }
        // ----- 初始化工作
        List<String> resultList = new ArrayList<>(100);
        Node currentNode = new Node(query.sourceID, query.source, 0.0);
        double sourceEndDistance = sourceLandmarkState.get(query.targetID);;
        double targetEndDistance = targetLandmarkState.get(query.targetID);;
        String goalID = query.targetID;
        S2Point goal = query.target;
        closeMap.put(query.sourceID, 0.0);
        // -----

        //---test
//        double total = 0;
//        double better = 0;
        //---
        // -----算法开始
        while (!currentNode.dataIndex.equals(goalID)) {
            for (Node node : TopologyGraph.getUnclosedLinkedNode(currentNode, closeMap)) {
                // openMap中不存在说明是第一次被扩展到，计算启发函数值，加入openMap
                if (!openMap.containsKey(node.dataIndex)) {
                    // landmarkState可能返回为null，因为我们使用的是local landmark。用optional包装
                    Optional<Double> optS = Optional.ofNullable(sourceLandmarkState.get(node.dataIndex));
                    // 不为null，计算lower bound，否则使用三维直线距离当作逃生，计算lower bound
                    // Math.abs的本质是Math.max(d(l, a) - d(l, b), d(l, b) - d(l, a))
                    double eDistance = getDistance(node, goal);
                    double heuristics = optS.map(distance -> Math.abs(sourceEndDistance - distance))
                            .orElseGet(() -> getDistance(node, goal));
                    if (heuristics < eDistance) {
                        heuristics = eDistance;
                    }

                    Optional<Double> optT = Optional.ofNullable(targetLandmarkState.get(node.dataIndex));
                    double heuristics2 = optT.map(distance -> Math.abs(targetEndDistance - distance))
                            .orElseGet(() -> getDistance(node, goal));
                    if (heuristics < heuristics2) {
                        heuristics = heuristics2;
                    }
                    // --- test begin
//                    total += 1;
//                    double distance = getDistance(node , goal);
//                    if (heuristics > distance) {
//                        logger.info("h:" + heuristics + "   d:" + distance);
//                        better += 1;
//                    }
                    // --- test end
                    node.total = node.gCost + heuristics;
                    node.parent = currentNode;
                    openMap.put(node.dataIndex, node);
                    minHeap.add(node);
                } else {
                    // 如果当前的扩展出去的节点其路径由于之前扩展到此节点的路径，则进行更新
                    if (openMap.get(node.dataIndex).gCost > node.gCost) {
                        Node revisedNode = openMap.get(node.dataIndex);
                        revisedNode.total = revisedNode.total - (revisedNode.gCost - node.gCost);
                        revisedNode.gCost = node.gCost;
                        revisedNode.parent = currentNode;
                        minHeap.swim(revisedNode);
                    }
                }
            }
            // 更新currentNode，closeMap，openMap
            if (!openMap.isEmpty()) {
                currentNode = minHeap.delMin();
                closeMap.put(currentNode.dataIndex, currentNode.gCost);
                openMap.remove(currentNode.dataIndex);
            } else {
                return new ArrayList<>();
            }
        }
        // ----- 算法结束

        isUsed = true;
        // 生成结果路径
        while (currentNode != null) {
            resultList.add(currentNode.dataIndex);
            currentNode = currentNode.parent;
        }
//        logger.info("加速比例:" + String.valueOf(better / total) + "   total:" + total + "   resultSize:" + resultList.size()
//                + "   openMapSize:" + openMap.size() + "   closeMap:" + closeMap.size() + "   length:" +
//                getDistance(new Node("1", query.source, 0.0), query.target));
        return resultList;
    }

    public List<String> getAstarShortestPath(Query query) {
        if (isUsed) {
            refresh();
        }
        // ----- 初始化
        List<String> resultList = new ArrayList<>(100);
        Node currentNode = new Node(query.sourceID, query.source, 0.0);
        String goalID = query.targetID;
        S2Point goal = query.target;
        // -----

//        double total = 0;
        // ----- 算法开始
        while (!currentNode.dataIndex.equals(goalID)) {
            for (Node node : TopologyGraph.getUnclosedLinkedNode(currentNode, closeMap)) {
                if (!openMap.containsKey(node.dataIndex)) {
                    double heuristics = getDistance(node, goal);
//                    total += 1;
                    node.total = node.gCost + heuristics;
                    node.parent = currentNode;
                    openMap.put(node.dataIndex, node);
                    minHeap.add(node);
                } else {
                    if (openMap.get(node.dataIndex).gCost > node.gCost) {
                        Node revisedNode = openMap.get(node.dataIndex);
                        revisedNode.total = revisedNode.total - (revisedNode.gCost - node.gCost);
                        revisedNode.gCost = node.gCost;
                        revisedNode.parent = currentNode;
                        minHeap.swim(revisedNode);
                    }
                }
            }
            if (!openMap.isEmpty()) {
                currentNode = minHeap.delMin();
                closeMap.put(currentNode.dataIndex, currentNode.gCost);
                openMap.remove(currentNode.dataIndex);
            } else {
                return new ArrayList<>();
            }
        }
        // -----

        isUsed = true;
        // 生成结果路径
        while (currentNode != null) {
            resultList.add(currentNode.dataIndex);
            currentNode = currentNode.parent;
        }
//        logger.info("A*:" + String.valueOf(total) + "   query:" + query.sourceID);
//        logger.info("A*:" + String.valueOf(resultList));
        return resultList;
    }

    /*
    dijkstra只是负责计算landmark。所以不需要生成结果路径
     */
    public void getDijkstraShortestPath(Query query) {
        if (isUsed) {
            refresh();
        }
        // ----- 初始化
        Node currentNode = new Node(query.sourceID, query.source, 0.0);
        String goalID = query.targetID;
        // -----

        // ----- 算法开始
        while (!currentNode.dataIndex.equals(goalID)) {
            for (Node node : TopologyGraph.getUnclosedLinkedNode(currentNode, closeMap)) {
                if (!openMap.containsKey(node.dataIndex)) {
                    node.total = node.gCost;
                    node.parent = currentNode;
                    openMap.put(node.dataIndex, node);
                    minHeap.add(node);
                } else {
                    if (openMap.get(node.dataIndex).gCost > node.gCost) {
                        Node revisedNode = openMap.get(node.dataIndex);
                        revisedNode.total = revisedNode.total - (revisedNode.gCost - node.gCost);
                        revisedNode.gCost = node.gCost;
                        revisedNode.parent = currentNode;
                        minHeap.swim(revisedNode);
                    }
                }
            }
            if (!openMap.isEmpty()) {
                currentNode = minHeap.delMin();
                closeMap.put(currentNode.dataIndex, currentNode.gCost);
                openMap.remove(currentNode.dataIndex);
            } else {
                return;
            }
        }
        // -----
        isUsed = true;
    }

    public HashMap<String, Double> getCloseMap() {
        return closeMap;
    }

    /*
    注意是创建新的变量，而不是clear()清空，因为landmarkState还在使用第一次计算的closeMap的引用
    因为value是引用类型时putAll方法添加的是浅拷贝
    这样可以防止以后代码扩展时，使用同一个pathCalculator意外的清空了可能需要保存的之前的计算结果
    至于内存回收，就让JVM去做吧
     */
    private void refresh() {
        this.openMap = new HashMap<>(capacity);
        this.closeMap = new HashMap<>(capacity);
        this.minHeap = new BinaryMinHeap<>(Node.class, capacity / 2);
        this.isUsed = false;
    }

    /*
    计算两点的三维空间距离作为启发函数值使用(在当前local landmark不包含LinkedNode的情况下使用)
    为方便算法不用转换，直接调用，输入的参数为Node和S2Point
     */
    private double getDistance2(Node source, S2Point target) {
        return source.position.getDistance2(target);
    }

    // 邻接表中weight以0.1m为单位
    private double getDistance(Node source, S2Point target) {
        S1Angle s1Angle = new S1Angle(source.position, target);
        return s1Angle.distance(63710000);
    }
}
