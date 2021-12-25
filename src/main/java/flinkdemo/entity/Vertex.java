package flinkdemo.entity;

import com.google.common.geometry.S2Point;

/*
作为S2Point的子类，在S2Point基础上增添了dataIndex(主键)以及nextNode(邻接表指针)
***注意构造函数中，super()或者this()必须放在第一行
 */
public class Vertex extends S2Point {
    public String dataIndex;
    public NextNode nextNode;

    public Vertex(){}

    // 输入主键以及表示三维向量的三个夹角，得到具体的xyz三维坐标生成S2Point
    public Vertex(String dataIndex, double phi, double theta, double cosphi){
        super(Math.cos(theta) * cosphi, Math.sin(theta) * cosphi, Math.sin(phi));
        this.dataIndex = dataIndex;
    }

    public String getDataIndex() {
        return dataIndex;
    }

    public void setDataIndex(String dataIndex) {
        this.dataIndex = dataIndex;
    }

    public NextNode getNextNode() {
        return nextNode;
    }

    public void setNextNode(NextNode nextNode) {
        this.nextNode = nextNode;
    }

    @Override
    public String toString() {
        return "Vertex{" +
                toDegreesString() +
                ", dataIndex='" + dataIndex + '\'' + '}';
    }
}
