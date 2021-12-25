package flinkdemo.entity;

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

    public Path(int pathID, List<String> sequence, double length2) {
        this.pathID = pathID;
        this.sequence = sequence;
        this.count = 0;
        this.length2 = length2;
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
