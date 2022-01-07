package flinkdemo.entity;

import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polyline;
import flinkdemo.util.TopologyGraph;

import java.util.ArrayList;
import java.util.List;

/*
作为local cache存储路径序列的pojo
 */
public class Path {
    // 路径
    public int pathID;
    public List<String> sequence;
    public int count;
    public double length2;

    public Path() {

    }

    public Path(int pathID) {
        this.pathID = pathID;
    }

    public Path(int pathID, List<String> sequence, int count) {
        this.pathID = pathID;
        this.sequence = sequence;
        this.count = count;
        this.length2 = 0;
    }

    public int getPathID() {
        return pathID;
    }

    public void setPathID(int pathID) {
        this.pathID = pathID;
    }

    public List<String> getSequence() {
        return sequence;
    }

    public void setSequence(List<String> sequence) {
        this.sequence = sequence;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getLength2() {
        return length2;
    }

    public void setLength2(double length2) {
        this.length2 = length2;
    }

    public void count() {
        this.count += 1;
    }

    public S2Point getFirstVertex() {
        return TopologyGraph.getVertex(sequence.get(0));
    }

    public S2Point getLastVertex() {
        return TopologyGraph.getVertex(sequence.get(sequence.size() - 1));
    }

    public S2Polyline toPolyline() {
        List<S2Point> pointList = new ArrayList<>();
        for (String dataIndex : sequence) {
            pointList.add(TopologyGraph.getVertex(dataIndex));
        }
        return new S2Polyline(pointList);
    }

    @Override
    public String toString() {
        return "Path{" +
                "pathID=" + pathID +
                ", sequence=" + sequence +
                ", count=" + count +
                ", length2=" + length2 +
                '}';
    }
}
