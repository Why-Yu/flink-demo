package flinkdemo.entity;

import java.util.List;
import java.util.Set;

public class LeafNode256 implements TreeNode{
    public LeafNode[] pointers;

    private byte size;

    public LeafNode256() {
        this.pointers = new LeafNode[256];
        this.size = 0;
    }

    @Override
    public TreeNode getByPartialKey(short partialKey) {
        return pointers[partialKey];
    }
    /**
     *只有非叶子节点才会调用这个，所以在这里此函数只是占位
     */
    @Override
    public TreeNode insert(short partialKey) {
        return null;
    }

    /**
     *只有非叶子节点才会调用这个，所以在这里此函数只是占位
     */
    @Override
    public TreeNode insert(short partialKey, boolean isLeaf) {
        return null;
    }

    /**
     *利用缓冲区的思路，在添加值节点时，也会将上一层级的周边八个格网也赋予相同的值
     * 从而达到允许误差下的的模糊匹配(平均误差应该在1.5%左右)
     * 误差计算:请求平均长度40km,单次匹配平均误差300m,起终点都需要匹配一次，故总平均误差600m
     */
    @Override
    public TreeNode insert(Set<Short> partialKeys, int pathID, int sequencePos) {
        for (short partialKey : partialKeys) {
            // 对没有赋值过的格网进行赋值
            if (pointers[partialKey] == null) {
                pointers[partialKey] = new LeafNode(pathID, sequencePos);
                ++size;
            // 对已经被不同缓存路径赋值过的格网进行当前缓存路径的赋值
            } else if (!pointers[partialKey].getPathID().contains(pathID)) {
                pointers[partialKey].getPathID().add(pathID);
                pointers[partialKey].getLeafValue().add(sequencePos);
            }
        }
        // 这个函数的返回值没有意义，直接返回null即可
        return null;
    }

    /**
     * 删除的时候必须进行pathID的校验，否则字典树会出问题
     */
    @Override
    public void delete(short partialKey, int pathID) {
        if (pointers[partialKey] != null && pointers[partialKey].getPathID().contains(pathID)) {
            if (pointers[partialKey].getPathID().size() == 1) {
                pointers[partialKey] = null;
                --size;
            } else {
                pointers[partialKey].delete(partialKey, pathID);
            }
        }
    }

    /**
     *叶子节点没有更新的需求，所以函数体为空
     */
    @Override
    public void update(short partialKey, TreeNode newNode) {

    }

    @Override
    public TreeNode grow() {
        return null;
    }

    @Override
    public TreeNode shrink(TreeNode parentNode) {
        return null;
    }

    @Override
    public boolean isFull() {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public List<Integer> getPathID() {
        return null;
    }

    @Override
    public List<Integer> getLeafValue() {
        return null;
    }
}
