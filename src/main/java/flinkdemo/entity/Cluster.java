package flinkdemo.entity;

import com.google.common.geometry.S2Point;

/*
利用球面上的cap来表达query的搜索空间
 */
public class Cluster implements Comparable<Cluster>{
    // 聚簇的唯一标识符
    public String clusterID;
    // 表示聚簇覆盖范围的cap
    public MyEllipse boundEllipse;

    public Cluster() {

    }

    public Cluster(String clusterID, S2Point s2PointS, S2Point s2PointT, double eccentricity) {
        this.clusterID = clusterID;
        this.boundEllipse = new MyEllipse(s2PointS, s2PointT, eccentricity);
    }

    /*
    检查一个聚簇是否包含另一个聚簇
    两端点在boundEllipse中
     */
    public boolean contains(Cluster other) {
        return boundEllipse.contains(other.boundEllipse.majorEndPointS) &&
                boundEllipse.contains(other.boundEllipse.majorEndPointT);
    }

    /*
    其实就是重写了Double.compare()方法,不过要使用降序,所以小于返回1
     */
    @Override
    public int compareTo(Cluster o) {
        if (this.boundEllipse.constant < o.boundEllipse.constant)
            return 1;
        if (this.boundEllipse.constant > o.boundEllipse.constant)
            return -1;

        long thisBits = Double.doubleToLongBits(this.boundEllipse.constant);
        long anotherBits = Double.doubleToLongBits(o.boundEllipse.constant);
        return (Long.compare(anotherBits, thisBits));
    }

    public String getClusterID() {
        return clusterID;
    }

    public void setClusterID(String clusterID) {
        this.clusterID = clusterID;
    }

    public MyEllipse getBoundEllipse() {
        return boundEllipse;
    }

    public void setBoundEllipse(MyEllipse boundEllipse) {
        this.boundEllipse = boundEllipse;
    }

    @Override
    public String toString() {
        return "Cluster{" +
                "clusterID='" + clusterID + '\'' +
                ", boundEllipse=" + boundEllipse +
                '}';
    }
}
