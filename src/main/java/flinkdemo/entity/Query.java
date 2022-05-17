package flinkdemo.entity;

import com.google.common.geometry.S2Point;

import java.io.Serializable;

public class Query implements Serializable {
    // 此值有两个功能
    // 1、因为keyed state无法在非 keyedStream上使用，所以在数据源上强行对query来一个逻辑分区，让query能分流
    // 2、WindowRouter中重用，使得可以根据重新赋值的此变量作为key,将请求分发到具体的角度窗口
    public int partition = 0;
    // 选择Sting是因为，可以让ID有各种表示方式，可以让类更具有泛用性，
    // 其实用int也是可以的
    public String sourceID;
    public String targetID;
    // query利用S2Point存储起终点
    public S2Point source;
    public S2Point target;

    public double length;
    public boolean cacheable = false;

    public String clusterID = "NoMatched";

    private static final long serialVersionUID = 1L;

    public Query() {

    }

    public Query(int partition, String sourceID, String targetID) {
        this.partition = partition;
        this.sourceID = sourceID;
        this.targetID = targetID;
    }

    public Query(String sourceID, String targetID, S2Point source, S2Point target) {
        this.sourceID = sourceID;
        this.targetID = targetID;
        this.source = source;
        this.target = target;
    }

    /**
     * 获得与当前query起终点相反的query
     */
    public Query getOppositeQuery() {
        return new Query(targetID, sourceID, target, source);
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public String getSourceID() {
        return sourceID;
    }

    public void setSourceID(String sourceID) {
        this.sourceID = sourceID;
    }

    public String getTargetID() {
        return targetID;
    }

    public void setTargetID(String targetID) {
        this.targetID = targetID;
    }

    public S2Point getSource() {
        return source;
    }

    public void setSource(S2Point source) {
        this.source = source;
    }

    public S2Point getTarget() {
        return target;
    }

    public void setTarget(S2Point target) {
        this.target = target;
    }

    public double getLength() {
        return length;
    }

    public void setLength(double length) {
        this.length = length;
    }

    public boolean isCacheable() {
        return cacheable;
    }

    public void setCacheable(boolean cacheable) {
        this.cacheable = cacheable;
    }

    public String getClusterID() {
        return clusterID;
    }

    public void setClusterID(String clusterID) {
        this.clusterID = clusterID;
    }

    @Override
    public String toString() {
        return "Query{" +
                "partition=" + partition +
                ", sourceID='" + sourceID + '\'' +
                ", targetID='" + targetID + '\'' +
                ", source=" + source +
                ", target=" + target +
                ", length=" + length +
                ", cacheable=" + cacheable +
                ", clusterID=" + clusterID +
                '}';
    }
}
