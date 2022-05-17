package flinkdemo.entity;

/**
 * 有一个设计细节是，指针数组的长度是49，而我们这个节点的容量本来只是想设计为48
 * 目的是为了解决byte数组默认值为0带来了误判问题
 * 因为byte数组默认值为0，所以导致无法判断究竟是存在实际的到位置0的映射还是只是单纯默认值
 * 故位置0置空作为哨兵，位置1-48存储真正的指针
 */
public class TreeNode48 implements TreeNode{
    public short[] partialKeys;
    public TreeNode[] pointers;

    private byte size;

    public TreeNode48() {
        this.partialKeys = new short[256];
        this.pointers = new TreeNode[49];
        this.size = 0;
    }

    public TreeNode48(byte size) {
        this.partialKeys = new short[256];
        this.pointers = new TreeNode[49];
        this.size = size;
    }

    @Override
    public TreeNode getByPartialKey(short partialKey) {
        return pointers[partialKeys[partialKey]];
    }

    @Override
    public TreeNode insert(short partialKey) {
        size++;
        partialKeys[partialKey] = size;
        TreeNode newNode = new TreeNode4();
        pointers[size] = newNode;
        return newNode;
    }

    @Override
    public TreeNode insert(short partialKey, boolean isLeaf) {
        size++;
        partialKeys[partialKey] = size;
        TreeNode newNode = new LeafNode256();
        pointers[size] = newNode;
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
        pointers[partialKeys[partialKey]] = null;
        partialKeys[partialKey] = 0;
        --size;
    }

    @Override
    public void update(short partialKey, TreeNode newNode) {
        short index = partialKeys[partialKey];
        if (index != 0) {
            pointers[index] = newNode;
        }
    }

    /**
     *从node48扩展为node256
     */
    @Override
    public TreeNode grow() {
        TreeNode256 treeNode256 = new TreeNode256((byte) 48);
        for (int index = 0 ; index < 256 ; ++index) {
           if (partialKeys[index] != 0) {
               treeNode256.pointers[index] = pointers[partialKeys[index]];
           }
        }
        return treeNode256;
    }

    @Override
    public TreeNode shrink(TreeNode parentNode) {
        return null;
    }

    @Override
    public boolean isFull() {
        return size == 48;
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
