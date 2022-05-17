package flinkdemo.entity;

public class TreeNode256 implements TreeNode{
    public TreeNode[] pointers;

    private byte size;

    public TreeNode256() {
        this.pointers = new TreeNode[256];
        this.size = 0;
    }

    public TreeNode256(byte size) {
        this.pointers = new TreeNode[256];
        this.size = size;
    }

    @Override
    public TreeNode getByPartialKey(short partialKey) {
        return pointers[partialKey];
    }

    @Override
    public TreeNode insert(short partialKey) {
        TreeNode newNode = new TreeNode4();
        pointers[partialKey] = newNode;
        ++size;
        return newNode;
    }

    @Override
    public TreeNode insert(short partialKey, boolean isLeaf) {
        TreeNode newNode = new LeafNode256();
        pointers[partialKey] = newNode;
        ++size;
        return newNode;
    }

    /**
     *只有叶子节点才会调用这个，所以在这里此函数只是占位
     */
    @Override
    public TreeNode insert(short partialKey, int pathID, int sequencePos) {
        return null;
    }

    @Override
    public void delete(short partialKey, int pathID) {
        pointers[partialKey] = null;
        --size;
    }

    @Override
    public void update(short partialKey, TreeNode newNode) {
        pointers[partialKey] = newNode;
    }

    /**
     *容量256已经包含当前层级格网下所有空间，故无需扩展
     */
    @Override
    public TreeNode grow() {
        return null;
    }

    @Override
    public TreeNode shrink(TreeNode parentNode) {
        return null;
    }

    /**
     *isFull()永远返回false，这样子radixTree就不会让treeNode256进入grow环节
     * 也就在代码统一的情况下，避免了出现空指针错误
     */
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
        return false;
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
