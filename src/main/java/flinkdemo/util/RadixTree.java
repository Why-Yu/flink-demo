package flinkdemo.util;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2Point;
import flinkdemo.entity.Path;
import flinkdemo.entity.TreeNode;
import flinkdemo.entity.TreeNode256;

import java.util.*;

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

            // 获取缓冲区插入的所有cell
            Map<S2CellId, Set<Short>> allNeighbors = getAllNeighbors(s2CellId);
            for (Map.Entry<S2CellId, Set<Short>> entry : allNeighbors.entrySet()) {
                // 插入前三层节点
                TreeNode currentNode = insertPreNode(entry.getKey());
                // 插入叶子节点
                currentNode.insert(entry.getValue(), pathID, count - 1);
            }
        }
    }

    /**
     * 为减少缓冲区插入时，对邻居格网相同的父格网重复的检索，故先把前三层级的节点插入妥当
     * 插入采用懒扩展设计，当需要插入时才进行扩展校验
     *  返回LeafNode256
     */
    private TreeNode insertPreNode(S2CellId s2CellId) {
        int level = 1;
        short partialKey;
        TreeNode prevNode = root;
        TreeNode currentNode = root;

        while (level < MAX_LEVEL) {
            partialKey = s2CellId.getPartialKey(level);
            TreeNode tempNode = currentNode.getByPartialKey(partialKey);

            if (tempNode == null) {
                if (currentNode.isFull()) {
                    TreeNode newNode = currentNode.grow();
                    // 要在上一层级更新引用，故partialKey也需要使用上一层级
                    short prePartialKey = s2CellId.getPartialKey(level - 1);
                    prevNode.update(prePartialKey, newNode);
                    currentNode = newNode;
                }

                prevNode = currentNode;
                if (level == 3) {
                    currentNode = currentNode.insert(partialKey, true);
                    break;
                } else {
                    currentNode = currentNode.insert(partialKey);
                }
            } else {
                prevNode = currentNode;
                currentNode = tempNode;
            }
            level++;
        }
        return currentNode;
    }

    /**
     * 获取当前点在level 15级上的8个邻居，加上自己的level 15父格网，形成9宫格存储在outPut中
     * 这9个cell覆盖的范围就作为我们某一点的缓冲区，其下有36个level 16cell，
     * 在字典树中这些cell可能分布在不同的第三层级节点下
     * 故最后以level 12即字典树第三层的partialKey为键，第四层partialKey为值返回结果
     */
    private Map<S2CellId, Set<Short>> getAllNeighbors(S2CellId s2CellId) {
        // S2CellId.getAllNeighbors函数会返回相同格网，原因不明，故只能多次调用getEdgeNeighbors
        S2CellId s2CellParent = s2CellId.parent(15);
        S2CellId[] neighbors = new S2CellId[4];
        s2CellParent.getEdgeNeighbors(neighbors);
        S2CellId down = neighbors[0];
        S2CellId up = neighbors[2];
        List<S2CellId> outPut = new ArrayList<>(Arrays.asList(neighbors));
        outPut.add(s2CellParent);
        down.getEdgeNeighbors(neighbors);
        outPut.add(neighbors[1]);
        outPut.add(neighbors[3]);
        up.getEdgeNeighbors(neighbors);
        outPut.add(neighbors[1]);
        outPut.add(neighbors[3]);

        Map<S2CellId, Set<Short>> allNeighbors = new HashMap<>();
        for (S2CellId s2CellId1 : outPut) {
            S2CellId parentId = s2CellId1.parent(12);
            if (allNeighbors.containsKey(parentId)) {
                Set<Short> neighborCell = allNeighbors.get(parentId);
                for (S2CellId s2CellId2 : s2CellId1.children()) {
                    neighborCell.add(s2CellId2.getPartialKey(4));
                }
            } else {
                Set<Short> neighborCell = new HashSet<>();
                for (S2CellId s2CellId2 : s2CellId1.children()) {
                    neighborCell.add(s2CellId2.getPartialKey(4));
                }
                allNeighbors.put(parentId, neighborCell);
            }
        }
        return allNeighbors;
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
            S2CellId s2DeleteCellId = S2CellId.fromPoint(s2Point);

            Map<S2CellId, Set<Short>> allNeighbors = getAllNeighbors(s2DeleteCellId);
            for (Map.Entry<S2CellId, Set<Short>> entry : allNeighbors.entrySet()) {
                S2CellId s2CellId = entry.getKey();
                int level = 1;
                short partialKey;
                TreeNode currentNode = root;
                // 根节点无需保存
                TreeNode[] nodeList = new TreeNode[3];
                short[] partialKeyList = new short[3];
                // 由于我们执行的是缓冲区删除，故删除的时候可能会把周边的节点也一起删除，造成后续节点使用getByPartialKey时返回空指针
                // 我们利用此变量来进行安全的删除操作
                boolean isDeleteBefore = false;

                // 从上到下逐层保存信息
                while (level < MAX_LEVEL) {
                    partialKey = s2CellId.getPartialKey(level);
                    partialKeyList[level - 1] = partialKey;
                    currentNode = currentNode.getByPartialKey(partialKey);
                    // 如果返回空指针，至少说明包含此节点的LeafNode256即level 12或者更高层级的父节点已经被删除了
                    // 所以后续计算全部跳过即可
                    if (currentNode == null) {
                        isDeleteBefore = true;
                        break;
                    }
                    nodeList[level - 1] = currentNode;
                    ++level;
                }

                if (!isDeleteBefore) {
                    // 从下到上，逐层删除(如果有一层不是空的，则可以直接跳出循环)
                    for (int i = 2 ; i >= 0 ; --i) {
                        if (i == 2) {
                            TreeNode tempNode = nodeList[i];
                            for (short key : entry.getValue()) {
                                tempNode.delete(key, pathID);
                            }
                        } else {
                            nodeList[i].delete(partialKeyList[i + 1], pathID);
                        }
                        // 若非空，则无需上一层级删除
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
    }

}
