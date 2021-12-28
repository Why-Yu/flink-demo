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

    public PathCalculator() {
        this.openMap = new HashMap<>(512);
        this.closeMap = new HashMap<>(512);
        this.minHeap = new BinaryMinHeap<>(Node.class, 256);
    }

    public List<String> getLandmarkShortestPath(Query query, MapState<String, Double> landmarkState, boolean direction) throws Exception {
        // 如果路径计算器已经被使用过，把map刷新一下
        // 但实际上我们现有代码一次query只会调用一次路径计算，并不会重复使用路径计算器(这样写是为了以后扩展时，防止出现意外错误)
        if (isUsed) {
            refresh();
        }
        // ----- 初始化工作
        List<String> resultList = new ArrayList<>(200);
        Node currentNode;
        double viewDistance;
        String goalID;
        S2Point goal;
        // 如果是true，表明以target作为搜索终点，则初始当前节点为source
        if (direction) {
            viewDistance = landmarkState.get(query.targetID);
            goalID = query.targetID;
            goal = query.target;
            currentNode = new Node(query.sourceID, query.source, 0.0);
            closeMap.put(query.sourceID, 0.0);
        } else { //否则以source作为搜索终点
            viewDistance = landmarkState.get(query.sourceID);
            goalID = query.sourceID;
            goal = query.source;
            currentNode = new Node(query.targetID, query.target, 0.0);
            closeMap.put(query.targetID, 0.0);
        }
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

    public List<String> getDijkstraShortestPath(Query query) {
        if (isUsed) {
            refresh();
        }
        // ----- 初始化
        List<String> resultList = new ArrayList<>(100);
        Node currentNode = new Node(query.sourceID, query.source, 0.0);
        String goalID = query.targetID;
        S2Point goal = query.target;
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
        return resultList;
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
        this.openMap = new HashMap<>(512);
        this.closeMap = new HashMap<>(512);
        this.minHeap = new BinaryMinHeap<>(Node.class, 256);
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
