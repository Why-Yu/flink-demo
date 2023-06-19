package flinkdemo;


public class test {
    public static void main(String[] args) throws Exception {
//        DataLoad.getGraph("F:\\road network benchmark\\USA-road-d.NY.co\\USA-road-d.NY.co",
//                "F:\\road network benchmark\\USA-road-d.NY.gr\\USA-road-d.NY.gr");
//        System.out.println(TopologyGraph.getDistance(TopologyGraph.getVertex("1"), TopologyGraph.getVertex("2")));
//        System.out.println(5 / 2);
//        List<Integer> testList = new ArrayList<>();
//        testList.add(1);
//        if (testList.contains(1)) {
//            System.out.println("yes");
//        }

//        Random random = new Random();
//        ConcurrentLinkedDeque<Query> deque = new ConcurrentLinkedDeque<>();
//        long startTime = System.nanoTime();
//
//        //创建一个线程并初始化
//        //具体的业务代码
//        Thread thread= new Thread() {
//            boolean flag = true;
//            @Override
//            public void run() {
//                while(flag) {
//                    // 随机发送query的逻辑处理分区
//                    int partition = random.nextInt(3) + 1;
////              int partition = 1;
//                    // 如果不加1,生成的随机数区间是[0,boundSize)
//                    // 我们需要的是[1,boundSize + 1)
//                    // 这里进行移位处理是因为我们想要模拟输入的请求是从固定的POI集合中选取的
//                    int source = ((random.nextInt(264346) >>> 3) << 3) + 1;
//                    int target = ((random.nextInt(264346) >>> 3) << 3) + 1;
//                    // 避免生成source和target相同的query
//                    while (source == target) {
//                        target = ((random.nextInt(264346) >>> 3) << 3) + 1;
//                    }
//                    deque.add(new Query(partition, String.valueOf(source), String.valueOf(target)));
//                    if ((System.nanoTime() - startTime) / 1000000 > 5000) {
//                        flag = false;
//                        System.out.println(deque.size());
//                    } else {
//                        try {
//                            Thread.sleep(30);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            }
//        };
//        //开启线程
//        thread.start();

//        Workbook wb = new HSSFWorkbook();
//        Workbook wb = new XSSFWorkbook();
//        CreationHelper createHelper = wb.getCreationHelper();
//        Sheet sheet = wb.createSheet("new sheet");
//        // 创建标题行
//        Row titleRow = sheet.createRow(0);
//        // Create a cell
//        titleRow.createCell(0).setCellValue(
//                createHelper.createRichTextString("第一分钟"));
//        titleRow.createCell(1).setCellValue(
//                createHelper.createRichTextString("第二分钟"));
//        titleRow.createCell(2).setCellValue(
//                createHelper.createRichTextString("第三分钟"));
//        titleRow.createCell(3).setCellValue(
//                createHelper.createRichTextString("第四分钟"));
//        titleRow.createCell(4).setCellValue(
//                createHelper.createRichTextString("第五分钟"));
//
//        Row row = sheet.createRow(1);
//        row.createCell(0).setCellValue(1.2);
//        // Write the output to a file
//        try (OutputStream fileOut = new FileOutputStream("F:\\road network benchmark\\full-NY-execution\\workbook.xlsx")) {
//            wb.write(fileOut);
//        }
//        File file = new File("F:\\road network benchmark\\yyy");
//        System.out.println(file.exists());
//        HashMap<String, Long> map = new HashMap<>();
//        map.put("1", 20L);
//        map.put("3", 30L);
//        ExcelWriter.addToExcel("F:\\road network benchmark\\full-NY-execution.xlsx", map, 0);
//        DataLoad.getGraph("F:\\road network benchmark\\USA-road-d.NY.co\\USA-road-d.NY.co",
//                "F:\\road network benchmark\\USA-road-d.NY.gr\\USA-road-d.NY.gr", 0.85);
//        PathCalculator pathCalculator = new PathCalculator();
//        long startTime = System.nanoTime();
//        pathCalculator.getAstarShortestPath(new Query("256297", "54489",
//                TopologyGraph.getVertex("256297"), TopologyGraph.getVertex("54489")));
//        long endTime = System.nanoTime();
//        System.out.println((endTime - startTime) / 1000000);
//        TxtWriter.cutData("F:\\road network benchmark\\small-NY.co", "F:\\road network benchmark\\small-NY.gr");

        Integer i = 125;
        Integer j = 125;
        System.out.println(i == j);
    }
}
