package flinkdemo.entity;

import com.google.common.geometry.*;

/* 重要(一些定义与思考)
在region接口上进一步扩展，主要是表示我们的近似椭圆搜索空间(感觉并不是一个椭圆)
本质上确定的搜索空间是椭球体和球体相交的截线所包围的部分球面
其中以source作为椭球体的majorEndPoint，target作为椭球体的另一个majorEndPoint
离心率由用户自定义，离心率越大，表示的搜索空间越小(近似椭圆越扁)
离心率要定义的稍微大一些,因为最坏情况下(比如离心率为0)就会退化为cap
离心率又不能定义的过大(一方面会造成之后形成过多聚簇，
一方面会使得椭球反而被球所包裹导致没有截线,a越大,e的上限就低，但是在我们小尺度的情况下，e取0.99都是没有问题的)

功能：在自适应粒度计算和聚簇判定都需要用到
 */
public strictfp class MyEllipse implements S2Region {
    // 椭球体的长半轴端点
    public final S2Point majorEndPointS;
    public final S2Point majorEndPointT;
    // 离心率
    public final double e;
    //下面三个变量是为了方便内部计算，所以在构造函数的时候就计算好，就不需要以后重复计算了
    // 常数2a，用于快速比较点是否落在椭圆内
    public final double constant;
    // 椭球体的焦点
    public final S2Point focusS;
    public final S2Point focusT;

    public MyEllipse() {
        this.majorEndPointS = new S2Point();
        this.majorEndPointT = new S2Point();
        this.e = 0.5;
        this.constant = majorEndPointS.getDistance(majorEndPointT);
        this.focusS = new S2Point();
        this.focusT = new S2Point();
    }

    public MyEllipse(S2Point source, S2Point target, double e) {
        this.majorEndPointS = source;
        this.majorEndPointT = target;
        this.e = e;
        this.constant = majorEndPointS.getDistance(majorEndPointT);
        S2Point axisInPlan = majorEndPointS.add(majorEndPointT).mul(0.5);
        this.focusS = majorEndPointS.sub(majorEndPointT).mul(e / 2).add(axisInPlan);
        this.focusT = majorEndPointT.sub(majorEndPointS).mul(e / 2).add(axisInPlan);
    }
    /*
    此函数在获取初始candidate时，负责快速获得初始候选cell(重要)
     */
    @Override
    public S2Cap getCapBound() {
        // 两个长半轴端点的中点作为axis,到中点的距离作为cap半径
        S2Point axis = majorEndPointS.add(majorEndPointT).normalize();
        return S2Cap.fromAxisChord(axis, new S1ChordAngle(majorEndPointS, axis));
    }

    /*
    球面近似椭圆的rectBound非常难算，所以我们简化一下吧，但regionCover貌似并没有这个函数(不重要)
    我们直接利用capBound计算rectBound，但带来的后果是计算出的rectBound会稍小一些
     */
    @Override
    public S2LatLngRect getRectBound() {
        return getCapBound().getRectBound();
    }

    /*
    完全包含一个cell,regionCover需要使用，用于判断一个cell是否需要继续扩展子cell
    如果完全包含，直接加入resultList(重要)
     */
    @Override
    public boolean contains(S2Cell cell) {
        S2Point[] vertices = new S2Point[4];
        for (int k = 0; k < 4; ++k) {
            vertices[k] = cell.getVertex(k);
            if (!contains(vertices[k])) {
                return false;
            }
        }
        return true;
    }

    /*
    regionCover间接需要使用，但这个函数是比较好实现的(重要)
    但contains(S2Cell cell)以及mayIntersect(S2Cell cell)需要使用
    点落在截面边界上也算作contains
     */
    @Override
    public boolean contains(S2Point p) {
        double distanceToFocusS = focusS.getDistance(p);
        double distanceToFocusT = focusT.getDistance(p);
        return distanceToFocusS + distanceToFocusT <= constant;
    }

    /*
    regionCover需要使用
    1、在生成新的candidate时，如果没有mayIntersect，直接不用生成新的candidate
    2、在向下扩展子cell时，用mayIntersect判断子cell是否需要expandChildren
    (非常重要)
    */
    @Override
    public boolean mayIntersect(S2Cell cell) {
        // cell很大,直接包含了我们的中心轴，那么不用接下去的计算，返回true
        S2Point axis = majorEndPointS.add(majorEndPointT).normalize();
        if (cell.contains(axis)) {
            return true;
        }

        S2Point[] vertices = new S2Point[4];
        for (int k = 0; k < 4; ++k) {
            vertices[k] = cell.getVertex(k);
            if (contains(vertices[k])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "MyEllipse{" +
                "majorEndPointS=" + majorEndPointS +
                ", majorEndPointT=" + majorEndPointT +
                ", e=" + e +
                ", constant=" + constant +
                ", focusS=" + focusS +
                ", focusT=" + focusT +
                '}';
    }
}
