package flinkdemo.util;

/*
因为flink的上下游算子没有提供高效的API来传递信息
所以我们自建一个类传递相关数据
 */
public class ParametersPasser {
    public static int granularity;
    public static double initialAverage;

    public static int getGranularity() {
        return granularity;
    }

    public static void setGranularity(int granularity) {
        ParametersPasser.granularity = granularity;
    }

    public static double getInitialAverage() {
        return initialAverage;
    }

    public static void setInitialAverage(double initialAverage) {
        ParametersPasser.initialAverage = initialAverage;
    }
}
