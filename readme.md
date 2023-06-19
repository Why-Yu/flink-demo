### 概述

> 此项目为小论文代码，主要实现最短路径的流处理，使用Flink API

#### 主要的思路为

* 先执行方向分流以及空间分流实现请求聚簇

* 再通过缓存层以及部分匹配最大程度减少后续的实际计算
* 最后通过ALLT方法对未匹配的最短路径请求部分实际计算
* 具体计算细节详见论文



```
operator
```

是我们自定义的流处理的算子，`QueryCluster`负责请求聚簇，`CacheAndLandmark`负责缓存生成、匹配，以及实际的最短路径计算

```
util
```

为了更快速的执行部分匹配以及最短路径计算，实现的一些基础工具类

主要由`PathCalculator`最短路径计算器，`RadixTree`自适应字典树，利用点所处的格网的希尔伯特编码实现查找，剩下是一些基于S2的点索引，格网ID计算