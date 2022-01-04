package flinkdemo.util;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import flinkdemo.entity.MyEllipse;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/*
载入对应路网拓扑数据到内存
 */
public class DataLoad {
    /*
    输入benchmark中的坐标文件以及距离文件，返回构建好的TopologyGraph
     */
    public static void getGraph(String coDataFileName, String grDataFileName, double eccentricity) throws Exception{
        Stream<String> coLines = Files.lines(Paths.get(coDataFileName));
        Stream<String> grLines = Files.lines(Paths.get(grDataFileName));

        coLines.forEach(ele -> {
            String[] arr = ele.split("\\s+");
            if (arr[0].equals("v")) {
                // 坐标数据集中，坐标精确到6位小数，直接使用fromE6最快
                S2LatLng s2LatLng = S2LatLng.fromE6(Integer.parseInt(arr[3]), Integer.parseInt(arr[2]));
                TopologyGraph.insertVertex(arr[1], s2LatLng);
            }
        });

        // 固定以经纬度(0,0)作为cap的固定中点
        S2Point s2PointS = new S2Point(1.0, 0.0, 0.0);
        // 路网的覆盖距离，以两个point的三维直线距离的平方定义
        S2Point s2PointT = S2LatLng.fromRadians(TopologyGraph.MaxLat - TopologyGraph.MinLat,
                TopologyGraph.MaxLng - TopologyGraph.MinLng).toPoint();

        // 计算的是cap的radius，记得必须在这里计算两点的中点作为半径的衡量
//        S2Point s2PointCenter =s2PointAxis.add(s2PointT).mul(0.5).normalize();
//        S2Cap s2Cap =S2Cap.fromAxisChord(s2PointAxis, new S1ChordAngle(s2PointAxis, s2PointCenter));
        MyEllipse myEllipse = new MyEllipse(s2PointS, s2PointT, eccentricity);

        // 初始化起始粒度以及起始平均值
        ParametersPasser.granularity = TopologyGraph.getGranularity(myEllipse);
        // 采用外接矩形长度的33%作为初始值
        ParametersPasser.initialAverage = s2PointS.getDistance(s2PointT) * 0.33;

        grLines.forEach(ele -> {
            String[] arr = ele.split("\\s+");
            if (arr[0].equals("a")) {
                // 注意：以下是最初想法，但是！！错误！！的，因为和的平方大于平方和，所以会导致eDistance >> heuristics
                // 距离数据集中，距离是球面距离且以0.1m作为基础单位，所以我们除10000，后面用1km作为基础单位
                // 同时经过我们验证，数据集采用地球半径6371km计算得到整数精确度的球面距离作为gr文件中的距离
                // 所以在我们的拓扑网络中，与S2Point保持一致，均使用单位球体，即再除6371完成归一化
                // 同时为节省后续计算资源，我们的距离保存的是平方值，之后统一比较平方值，而不是实际距离
//                TopologyGraph.insertEdge(arr[1], arr[2], Math.pow(Double.parseDouble(arr[3]) / 63710000, 2));

                //最短路径算法使用的weight和我们之前路由、聚簇的length2是独立的
                TopologyGraph.insertEdge(arr[1], arr[2], Double.parseDouble(arr[3]));
            }
        });
    }
}
