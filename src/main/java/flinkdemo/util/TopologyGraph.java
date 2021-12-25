package flinkdemo.util;

import com.google.common.geometry.S2Cap;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2RegionCoverer;
import flinkdemo.entity.MyEllipse;
import flinkdemo.entity.NextNode;
import flinkdemo.entity.Node;
import flinkdemo.entity.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
被迫把所有方法都改为static
因为flink无法对 List或者Map这种动态对象进行序列化，所以没有办法通过构造函数传递TopologyGraph实例对象
即使取消了内部类，也把这个类变量都修改为可以序列化的ArrayList和HashMap都会发生无法序列化的错误
那干脆都设为static当作工具类好了
!!!注意路网中存储的点是三维坐标，存储的边的权重是三维距离的平方
 */
public strictfp class TopologyGraph {
    public static double MaxLat = Double.NEGATIVE_INFINITY;
    public static double MinLat = Double.POSITIVE_INFINITY;
    public static double MaxLng = Double.NEGATIVE_INFINITY;
    public static double MinLng = Double.POSITIVE_INFINITY;

    /**
     * 初始容量4096，减少储存空间的频繁调整
     */
    private static List<Vertex> vertices = new ArrayList<>(4096);
    private static Map<String, Integer> dataIndexToVerticesIndex = new HashMap<>(4096);

    // 公用regionCoverer实例，并不需要每次都build新的实例
    private static final S2RegionCoverer regionCoverer = S2RegionCoverer.builder().setMinLevel(0)
            .setMaxLevel(20).setMaxCells(10).build();

    // 有序排列的角度窗口划分阈值(以15度为窗口)，方便后续二分查找
    private static final double[] thetaLookUpTable = {-1, -0.965926, -0.866025, -0.707107, -0.5, -0.258819, 0,
            0.258819, 0.5, 0.707107, 0.866025, 0.965926, 1};

    public TopologyGraph(){
    }

    public static Vertex getVertex(String dataIndex) {
        int verticesIndex = dataIndexToVerticesIndex.get(dataIndex);
        return vertices.get(verticesIndex);
    }

    /*
    ！！！注意：无法处理跨越经度为180这条线的数据，我们先忽略！！！
     */
    public static void insertVertex(String dataIndex, S2LatLng s2LatLng) {
        if(!dataIndexToVerticesIndex.containsKey(dataIndex)){
            dataIndexToVerticesIndex.put(dataIndex, vertices.size());
            Vertex vertex = new Vertex(dataIndex, s2LatLng.latRadians(), s2LatLng.lngRadians(),
                    Math.cos(s2LatLng.latRadians()));
            vertices.add(vertex);
            // 为本次任务新增的，别的任务需要删除此块内容，目的是为了获得路网的最小外接矩形
            // 处理跨经度线180的数据很麻烦，所以没考虑
            double latRadians = s2LatLng.latRadians();
            double lngRadians = s2LatLng.lngRadians();
            MaxLat = Math.max(latRadians, MaxLat);
            MinLat = Math.min(latRadians, MinLat);
            MaxLng = Math.max(lngRadians, MaxLng);
            MinLng = Math.min(lngRadians, MinLng);
        }
    }

    public static void deleteVertex(String dataIndex) {
        //在构建的时候其实是用不上的，但写着玩,但添加操作简单，删除操作一般都很麻烦
        if(dataIndexToVerticesIndex.containsKey(dataIndex)){
            int verticesIndex = dataIndexToVerticesIndex.get(dataIndex);
            vertices.set(verticesIndex, new Vertex());

            NextNode previous;
            NextNode current;
            for(Vertex vertex : vertices){
                NextNode nextNode = vertex.nextNode;
                if(nextNode == null) continue;
                if(nextNode.dataIndex.equals(dataIndex) && nextNode.nextNode == null){
                    vertex.nextNode = null;
                    continue;
                }
                //这时nextNode等于链条中的第一个点
                current = nextNode;
                while (current != null){
                    previous = current;
                    current = current.nextNode;
                    if(current.dataIndex.equals(dataIndex)){
                        previous.nextNode = current.nextNode;
                        break;
                    }
                }
            }
            dataIndexToVerticesIndex.remove(dataIndex);
        }
    }

    //插入新边时，直接插在Vertex后面，而不是一行链表的最后面
    //不能插入环
    //代码简化，不再需要对nextNode是否为null做检查
    public static void insertEdge(String dataIndex1, String dataIndex2, double weight) {
        if(dataIndexToVerticesIndex.containsKey(dataIndex1) &&
                dataIndexToVerticesIndex.containsKey(dataIndex2) && !dataIndex1.equals(dataIndex2)){
            //-----拒绝重复添加边
            int tempVerticesIndex = dataIndexToVerticesIndex.get(dataIndex1);
            NextNode tempNextNode = vertices.get(tempVerticesIndex).nextNode;
            while (tempNextNode != null){
                if(tempNextNode.dataIndex.equals(dataIndex2)) return;
                tempNextNode = tempNextNode.nextNode;
            }
            //-----
            int verticesIndex1 = dataIndexToVerticesIndex.get(dataIndex1);
            NextNode newNode = new NextNode(dataIndex2, weight);
            newNode.nextNode = vertices.get(verticesIndex1).nextNode;
            vertices.get(verticesIndex1).nextNode = newNode;

            int verticesIndex2 = dataIndexToVerticesIndex.get(dataIndex2);
            newNode = new NextNode(dataIndex1, weight);
            newNode.nextNode = vertices.get(verticesIndex2).nextNode;
            vertices.get(verticesIndex2).nextNode = newNode;
        }
    }

    public static void deleteEdge(String dataIndex1, String dataIndex2) {
        if(dataIndexToVerticesIndex.containsKey(dataIndex1) &&
                dataIndexToVerticesIndex.containsKey(dataIndex2)){
            int verticesIndex1 = dataIndexToVerticesIndex.get(dataIndex1);
            NextNode current = vertices.get(verticesIndex1).nextNode;
            NextNode previous = null;
            //这步的判断是必须的，此时恰好第一个nextNode就是想要的点，单独处理，防止我们引用空指针previous,下同
            if(current.dataIndex.equals(dataIndex2)) {
                vertices.get(verticesIndex1).nextNode = current.nextNode;
                current = null;
            }
            while(current != null && !current.dataIndex.equals(dataIndex2)){
                previous = current;
                current = current.nextNode;
            }
            if(current != null) previous.nextNode = current.nextNode;

            int verticesIndex2 = dataIndexToVerticesIndex.get(dataIndex2);
            current = vertices.get(verticesIndex2).nextNode;
            previous = null;
            if(current.dataIndex.equals(dataIndex1)) {
                vertices.get(verticesIndex2).nextNode = current.nextNode;
                current = null;
            }
            while(current != null && !current.dataIndex.equals(dataIndex1)){
                previous = current;
                current = current.nextNode;
            }
            if(current != null) previous.nextNode = current.nextNode;
        }
    }

    /*
    获取所有不在closeMap中并与当前节点相邻的节点(形成Node列表返回给路径计算器)
     */
    public static List<Node> getUnclosedLinkedNode(Node node, HashMap<String, Double> closeMap) {
        int verticesIndex = dataIndexToVerticesIndex.get(node.dataIndex);
        NextNode current = vertices.get(verticesIndex).nextNode;
        int tempVerticesIndex;
        Vertex tempVertex;
        // 因为路网是稀疏图，一般来说每个节点不会拥有超过4个邻接节点(4个表明此节点刚好位于十字路口中心)
        // 默认ArrayList的大小是10，通过此操作我们应该能省下一些资源
        List<Node> nodeList = new ArrayList<>(5);
        // 检查所有相邻节点
        while(current != null){
            tempVerticesIndex = dataIndexToVerticesIndex.get(current.dataIndex);
            tempVertex = vertices.get(tempVerticesIndex);
            // 如果closeMap没有包含此节点
            if (!closeMap.containsKey(current.dataIndex)) {
                Node tempNode = new Node(tempVertex.dataIndex, tempVertex, node.gCost + current.weight);
                nodeList.add(tempNode);
            }
            current = current.nextNode;
        }
        return nodeList;
    }

//    /**
//     * 用户输入坐标后，获得网络中对应的起止点
//     * @param floor
//     * @param x
//     * @param y
//     * @return 与输入最近的Node
//     */
//    public static Node findNearNode(int floor, double x, double y){
//        List<Vertex> vertexList = getVerticesInFloor(floor);
//        double tempMin = Double.MAX_VALUE;
//        double absolute;
//        String tempDataIndex = floor + "-0";
//
//        for(Vertex vertex : vertexList){
//            absolute = Math.abs(x - vertex.x) + Math.abs(y -vertex.y);
//            if(absolute <= tempMin){
//                tempMin = absolute;
//                tempDataIndex = vertex.dataIndex;
//            }
//        }
//        int verticesIndex = dataIndexToVerticesIndex.get(tempDataIndex);
//        return new Node(tempDataIndex, floor, vertices.get(verticesIndex).x, vertices.get(verticesIndex).y, 0);
//    }

//    public static List<Vertex> getVerticesInFloor(int floor){
//        List<Vertex> vertexList = new ArrayList<>();
//        for(Vertex vertex : vertices){
//            if(vertex.floor == floor)
//                vertexList.add(vertex);
//        }
//        return vertexList;
//    }

    public static int getNumOfVertices() {
        return vertices.size();
    }

    // 返回两个节点的三维直线距离，而不是球面距离，所以会略小于球面距离，两点距离越远差值越大
    public static double getDistance2(Vertex source, Vertex target) {
        return source.getDistance2(target);
    }

    /*
    给定一个cap范围，返回在以此cap为基本尺度下，我们应该使用的粒度
     */
    public static int getGranularity(MyEllipse myEllipse) {
        // 生成region cover
        ArrayList<S2CellId> cellIdArrayList = new ArrayList<>();
        regionCoverer.getCovering(myEllipse, cellIdArrayList);
        // 以cover中的最小级+1作为我们后续point所处cell的粒度
        int granularity = Integer.MAX_VALUE;
        for(S2CellId s2CellId : cellIdArrayList) {
            if(s2CellId.level() < granularity) {
                granularity = s2CellId.level();
            }
        }
        return granularity + 1;
    }

    public static int getGranularity(S2Cap s2Cap) {
        // 生成region cover
        ArrayList<S2CellId> cellIdArrayList = new ArrayList<>();
        regionCoverer.getCovering(s2Cap, cellIdArrayList);
        // 以cover中的最小级+1作为我们后续point所处cell的粒度
        int granularity = Integer.MAX_VALUE;
        for(S2CellId s2CellId : cellIdArrayList) {
            if(s2CellId.level() < granularity) {
                granularity = s2CellId.level();
            }
        }
        return granularity + 1;
    }

    public static int getThetaWindow(double theta) {
        return binarySearch(0, 12, theta);
    }

    /*
    利用二分法，查找key所在的角度窗口
     */
    private static int binarySearch(int low, int high, double key) {
        // 如果low > high说明查找结束，low - 1就是向量落在的区间
        if (low > high) {
            return low - 1;
        }
        int mid = Math.floorDiv(low + high, 2);
        // 若相等则划分为左侧分区
        if (key == thetaLookUpTable[mid]) {
            return mid;
        } else if (key < thetaLookUpTable[mid]) {
            return binarySearch(low, mid - 1, key);
        } else {
            return binarySearch(mid + 1, high, key);
        }
    }

    public static List<Vertex> getVertices() {
        return vertices;
    }

    public static void setVertices(List<Vertex> vertices) {
        TopologyGraph.vertices = vertices;
    }

    public static Map<String, Integer> getDataIndexToVerticesIndex() {
        return dataIndexToVerticesIndex;
    }

    public static void setDataIndexToVerticesIndex(Map<String, Integer> dataIndexToVerticesIndex) {
        TopologyGraph.dataIndexToVerticesIndex = dataIndexToVerticesIndex;
    }

    @Override
    public String toString() {
        return "TopologyNetwork{" +
                "vertices=" + vertices +
                '}';
    }
}
