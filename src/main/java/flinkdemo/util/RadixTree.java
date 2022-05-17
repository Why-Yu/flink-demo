package flinkdemo.util;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2Point;
import flinkdemo.entity.Path;
import flinkdemo.entity.TreeNode;
import flinkdemo.entity.TreeNode256;

import java.util.List;

/**
 * 整棵树的设计思路如下
 * 根节点和叶子节点默认256类型，以达到最高的搜索效率，中间节点采用可扩展的设计
 * 无论非叶子节点，叶子节点，值节点都实现TreeNode接口，实现代码统一
 * 整个字典树具体的查找、添加等功能由几个基本节点类型的具体函数实现进行组合
 */
public class RadixTree {
    private final TreeNode root;

    private static final int MAX_LEVEL = 4;


    public RadixTree() {
        this.root = new TreeNode256();
    }

    /**
     *此函数最频繁被调用，所以效率要求最高
     * 因为叶子节点有缓冲区插入的设计思路，
     * 所以最后找到的节点可能是精确匹配的也可能是一定误差容许下的模糊匹配(因为不一定要精确的最近距离点)
     * 故找不到对应节点就可以直接判空
     * 最后返回LeafNode(但以TreeNode接口返回以保证代码的统一性)
     */
    public TreeNode getByS2CellId(S2CellId s2CellId) {
        int level = 1;
        short partialKey;
        TreeNode currentNode = root;

        // 先寻找是否存在对应格网
        while (level <= MAX_LEVEL) {
            partialKey = s2CellId.getPartialKey(level);
            currentNode = currentNode.getByPartialKey(partialKey);
            // 在字典树中根本就找不到对应的非叶子节点或叶子节点
            if (currentNode == null) {
                return null;
            }
            level++;
        }
        return currentNode;
    }

    /**
     * 在插入字典树时，采用批量插入，故入参为Path
     * 插入采用懒扩展设计，当需要插入时才进行扩展校验
     * 具体的insert重载设计，不同场景调用不同insert
     */
    public void insert(Path path) {
        int pathID = path.pathID;
        List<String> pathSequence = path.sequence;
        int count = 0;

        for (String dataIndex : pathSequence) {
            // 隔一个点，进行插入计算，因为叶子节点有缓冲区设计
            // 这样应该可以适当减少一些计算量(同时又不至于丢失缓存命中率，leafNode256数组中多个指针指向的是同一个值，又可以节省内存)
            ++count;
            if ((count & 1) == 0) {
                continue;
            }

            S2Point s2Point = TopologyGraph.getVertex(dataIndex);
            S2CellId s2CellId = S2CellId.fromPoint(s2Point);

            int level = 1;
            short partialKey;
            TreeNode prevNode = root;
            TreeNode currentNode = root;

            while (level <= MAX_LEVEL) {
                partialKey = s2CellId.getPartialKey(level);
                TreeNode tempNode = currentNode.getByPartialKey(partialKey);

                if (tempNode == null) {
                    if (currentNode.isFull()) {
                        TreeNode newNode = currentNode.grow();
                        prevNode.update(partialKey, newNode);
                        currentNode = newNode;
                    }

                    prevNode = currentNode;
                    if (level == 4) {
                        currentNode.insert(partialKey, pathID, count - 1);
                        break;
                    } else if (level == 3) {
                        currentNode = currentNode.insert(partialKey, true);
                    } else {
                        currentNode = currentNode.insert(partialKey);
                    }
                } else {
                    prevNode = currentNode;
                    currentNode = tempNode;
                }
                level++;
            }
        }
    }

    /**
     *删除时先从上到下寻找路径保存信息
     * 再从下到上判断是否需要删除节点
     * 注意：节点容量的收缩出于代码编写方便，暂时未写
     */
    public void delete(Path path) {
        int pathID = path.pathID;
        List<String> pathSequence = path.sequence;
        int count = 0;

        for (String dataIndex : pathSequence) {
            ++count;
            if ((count & 1) == 0) {
                continue;
            }

            S2Point s2Point = TopologyGraph.getVertex(dataIndex);
            S2CellId s2CellId = S2CellId.fromPoint(s2Point);

            int level = 1;
            short partialKey;
            TreeNode currentNode = root;
            // 根节点无需保存
            TreeNode[] nodeList = new TreeNode[4];
            short[] partialKeyList = new short[4];

            // 从上到下逐层保存信息
            while (level <= MAX_LEVEL) {
                partialKey = s2CellId.getPartialKey(level);
                partialKeyList[level - 1] = partialKey;
                currentNode = currentNode.getByPartialKey(partialKey);
                nodeList[level - 1] = currentNode;
                ++level;
            }

            // 从下到上，逐层删除(如果有一层不是空的，则可以直接跳出循环)
            for (int i = 2 ; i >= 0 ; --i) {
                nodeList[i].delete(partialKeyList[i + 1], pathID);
                if (!nodeList[i].isEmpty()) {
                    break;
                }
            }
            if (nodeList[0].isEmpty()) {
                root.delete(partialKeyList[0], pathID);
            }
        }
    }

}
