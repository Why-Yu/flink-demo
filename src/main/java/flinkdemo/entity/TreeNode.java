package flinkdemo.entity;

/**
 * 字典树中每个节点的基本应完成的功能
 * (部分冗余，主要是想把指针节点和值节点统一起来)
 */
public interface TreeNode {
    /**
     * 依据64位ID中的部分8位获取对应的treeNode
     */
    TreeNode getByPartialKey(short partialKey);

    /**
     * 在非叶子节点上嵌入新的非叶子节点
     */
    TreeNode insert(short partialKey);

    /**
     * 在非叶子节点上嵌入新的叶子节点
     */
    TreeNode insert(short partialKey, boolean isLeaf);

    /**
     * 在叶子节点上，嵌入新的值节点
     */
    TreeNode insert(short partialKey, int pathID, int sequencePos);

    /**
     * 删除节点
     */
    void delete(short partialKey, int pathID);

    /**
     * 修改数组对应位置的引用
     */
    void update(short partialKey, TreeNode newNode);

    /**
     * 因为是adaptive Node，所以需要扩充节点容量
     */
    TreeNode grow();

    /**
     * 节点容量的收缩
     */
    TreeNode shrink(TreeNode parentNode);

    /**
     * 判断节点是否已满
     */
    boolean isFull();

    /**
     *判断节点是否为空
     */
    boolean isEmpty();

    /**
     * 判断是否是叶子节点
     */
    boolean isLeaf();

    /**
     * 叶子节点获取对应的pathID
     */
    int getPathID();

    /**
     * 叶子节点获取对应的点在缓存路径上的位置
     */
    int getLeafValue();


}
