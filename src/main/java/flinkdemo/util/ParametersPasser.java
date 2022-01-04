package flinkdemo.util;

/*
因为flink的上下游算子没有提供高效的API来传递信息
所以我们自建一个类传递相关数据
线程安全问题的解决比较简单，原因有二
--在修改方面，因为我们已经限制了只有某一个线程中的算子才有权力修改这里的数值
比如我们设windowsRouter中逻辑分区为1的子任务才有权力修改，不会有两个线程同时想要修改这里
所以不用考虑原子性问题
--在读取方面，由于在修改的时候，可能恰好某个后续算子在读取，但是读取到旧数据其实是没有关系的
读到旧数据无非是稍微不准一点，但是这个不准不会对结果造成什么影响
但我们还是使用volatile关键字保证可见性吧
 */
public class ParametersPasser {
    public static volatile int granularity;
    public static volatile double initialAverage;
    // 存的是length2
    public static volatile double maxDistance;

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

    public static double getMaxDistance() {
        return maxDistance;
    }

    public static void setMaxDistance(double maxDistance) {
        ParametersPasser.maxDistance = maxDistance;
    }
}
