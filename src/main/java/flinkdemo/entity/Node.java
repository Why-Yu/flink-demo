package flinkdemo.entity;

import com.google.common.geometry.S2Point;

/**
 *  Node是在路径计算器中算法需要使用的节点抽象
 *  Vertex是拓扑路网的存储抽象
 */
public class Node implements Comparable<Node>{
    /**
     * dataIndex:在数据库中的唯一id
     * arrayIndex:在二叉最小堆中的index
     * parent:在最短路径中到达这个结点的上一个结点
     * position:描述点的具体三维空间位置
     * total,gCost 此点到终点的估计值和起点到此点的实际值
     * heuristics = total - gCost
     */
    public String dataIndex;
    public int arrayIndex;

    public Node parent;
    public S2Point position;

    public double total;
    public double gCost;

    /**
     * 这个无参构造是必须的，不然MinHeap里就不能用Node类对象创建Node实例了
     */
    public Node(){

    }

    // 主要是在TopologyGraph.getUnclosedLinkedNode中调用
    public Node(String dataIndex, S2Point position, double gCost){
        this.dataIndex = dataIndex;
        this.position = position;
        this.gCost = gCost;
    }

    @Override
    public int compareTo(Node o) {
        return Double.compare(this.total, o.total);
    }

    @Override
    public String toString() {
        return "Node{" +
                "dataIndex='" + dataIndex + '\'' +
                ", arrayIndex=" + arrayIndex +
                ", position=" + position +
                ", total=" + total +
                ", gCost=" + gCost +
                '}';
    }
}
