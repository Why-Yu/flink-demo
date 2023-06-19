package flinkdemo.util;

import flinkdemo.util.visual.ExcelWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ScheduledStats {
    public static volatile boolean ready = true;
    private static final AtomicInteger round = new AtomicInteger(0);
    public final static ConcurrentHashMap<String, List<Long>> timeUsageDeque = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public final static ConcurrentLinkedDeque<Integer> mapSize2Landmark = new ConcurrentLinkedDeque<>();
    public final static ConcurrentLinkedDeque<Integer> mapSize1Landmark = new ConcurrentLinkedDeque<>();
    public final static ConcurrentLinkedDeque<Integer> mapSize0Landmark = new ConcurrentLinkedDeque<>();

    public final static ConcurrentHashMap<String, AtomicInteger> queryNumber = new ConcurrentHashMap<>();
    public final static ConcurrentHashMap<String, AtomicInteger> hitNumber = new ConcurrentHashMap<>();

    public static void addTimeUsage(String key, long time) {
        if(timeUsageDeque.containsKey(key)) {
            timeUsageDeque.get(key).add(time);
        } else {
            timeUsageDeque.putIfAbsent(key, new ArrayList<>());
        }
    }

    public static void addCloseMapSize(int mapSize, int landmarkNumber) {
        if (landmarkNumber == 2) {
            mapSize2Landmark.add(mapSize);
        } else if (landmarkNumber == 1) {
            mapSize1Landmark.add(mapSize);
        } else {
            mapSize0Landmark.add(mapSize);
        }
    }

    public static void addQueryNumber(String key, int num) {
        if(queryNumber.containsKey(key)) {
            queryNumber.get(key).addAndGet(num);
        } else {
            queryNumber.put(key, new AtomicInteger(num));
        }
    }

    public static void addHitNumber(String key, double num) {
        int numInt = (int) num;
        if(hitNumber.containsKey(key)) {
            hitNumber.get(key).addAndGet(numInt);
        } else {
            hitNumber.put(key, new AtomicInteger(numInt));
        }
    }

    public static HashMap<String, Long> getMeanExecutionMap() {
        HashMap<String, Long> resultMap = new HashMap<>();
        for (String key : timeUsageDeque.keySet()) {
            if (timeUsageDeque.get(key).size() != 0) {
                String windowKey = key.split("-")[0];
                long sumTime = 0;
                int size = timeUsageDeque.get(key).size();
                for(long time : timeUsageDeque.get(key)) {
                    sumTime += time;
                }
                if(resultMap.containsKey(windowKey)){
                    long preTime = resultMap.get(windowKey);
                    long clusterTime = sumTime / size;
                    resultMap.put(windowKey, (preTime + clusterTime) / 2);
                } else {
                    resultMap.put(windowKey, sumTime / size);
                }
            }
        }
        return resultMap;
    }

    public static void startTimeStats() {
        Runnable period = () -> {
            ready = false;
            HashMap<String, Long> resultMap = getMeanExecutionMap();
            try {
                ExcelWriter.addTimeToExcel("F:\\road network benchmark\\full-NY-execution.xlsx", resultMap, round.intValue());
            } catch (IOException e) {
                e.printStackTrace();
            }
            round.addAndGet(1);
            timeUsageDeque.clear();
            ready = true;
        };
        scheduler.scheduleAtFixedRate(period, 1, 1, TimeUnit.MINUTES);
    }

    public static void startMapSizeStats() {
        Runnable period = () -> {
            int meanNum2 = mapSize2Landmark.stream().mapToInt(num -> num).sum() / mapSize2Landmark.size();
            int meanNum1 = mapSize1Landmark.stream().mapToInt(num -> num).sum() / mapSize1Landmark.size();
            int meanNum0 = mapSize0Landmark.stream().mapToInt(num -> num).sum() / mapSize0Landmark.size();
            try {
                ExcelWriter.addMapSizeToExcel("F:\\road network benchmark\\mapSize.xlsx",
                        meanNum2, meanNum1, meanNum0);
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
        scheduler.schedule(period, 2, TimeUnit.MINUTES);
    }

    public static void startHitNumberStats() {
        Runnable period = () -> {
            int querySum = 0;
            int hitSum = 0;
            for (AtomicInteger value : queryNumber.values()) {
                querySum += value.intValue();
            }

            for (AtomicInteger value : hitNumber.values()) {
                hitSum += value.intValue();
            }
            try {
                ExcelWriter.addHitRatioToExcel("F:\\road network benchmark\\hitRatio.xlsx",
                        querySum, hitSum, round.intValue());
            } catch (IOException e) {
                e.printStackTrace();
            }
            round.addAndGet(1);
            queryNumber.clear();
            hitNumber.clear();
        };
        scheduler.scheduleAtFixedRate(period, 1, 1, TimeUnit.MINUTES);
    }
}
