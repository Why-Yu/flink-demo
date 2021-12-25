package flinkdemo.util.Interfaces;

import flinkdemo.entity.Node;
import java.util.List;

public interface Graph {
    //构建图的时候必须的四个函数
    void insertVertex(String  dataIndex, int floor, double x, double y);
    void deleteVertex(String dataIndex);
    void insertEdge(String dataIndex1, String dataIndex2, double weight);
    void deleteEdge(String dataIndex1, String dataIndex2);

    //算法过程中调用的函数
    /**
     * 获得当前节点的所有邻接节点
     * @param node 当前节点
     * @return 邻接的所有节点的列表
     */
    List<Node> getLinkedNode(Node node);

    //可能在测试的时候有用，先预备着
    int getNumOfVertices();
}
