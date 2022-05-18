package flinkdemo.entity;

public class LeafNode256 implements TreeNode{
    public LeafNode[] pointers;

    private byte size;

    private static final int ERROR_BOUND = 12;

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
     *利用缓冲区的思路，在添加值节点时，也会将周边的几个格网也赋予相同的值
     * 从而达到允许误差下的的模糊匹配
     */
    @Override
    public TreeNode insert(short partialKey, int pathID, int sequencePos) {
        LeafNode leafNode = new LeafNode(pathID, sequencePos);
        short left = (short) Math.max(partialKey - ERROR_BOUND, 0);
        short right = (short) Math.min(partialKey + ERROR_BOUND, 255);
        while (left <= right) {
            // 只对没有赋值过的格网进行赋值
            if (pointers[left] == null) {
                ++size;
                pointers[left] = leafNode;
            }
            ++left;
        }
        return leafNode;
    }

    /**
     *删除的时候必须进行pathID的校验，否则字典树会出问题
     */
    @Override
    public void delete(short partialKey, int pathID) {
        short left = (short) Math.max(partialKey - ERROR_BOUND, 0);
        short right = (short) Math.min(partialKey + ERROR_BOUND, 255);
        while (left <= right) {
            if (pointers[left] != null && pointers[left].getPathID() == pathID) {
                pointers[left] = null;
                --size;
            }
            ++left;
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
    public int getPathID() {
        return 0;
    }

    @Override
    public int getLeafValue() {
        return -1;
    }
}
