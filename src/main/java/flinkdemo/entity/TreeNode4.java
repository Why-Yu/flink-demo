package flinkdemo.entity;

/**
 * 对于TreeNode中的各个函数，我的设计思路是每个函数各司其职
 * 具体的逻辑组合放到radixTree中进行
 * 所以例如insert函数是完全不考虑容量已满的情况
 * short类型数组而不是byte类型数组的原因是java不存在无符号byte基本类型
 */
public class TreeNode4 implements TreeNode{
    public short[] partialKeys;
    public TreeNode[] pointers;

    private byte size;

    public TreeNode4() {
        partialKeys = new short[4];
        pointers = new TreeNode[4];
        size = 0;
    }

    /**
     * 最多只有4个，所以采用遍历
     * 找不到应该返回null
     */
    @Override
    public TreeNode getByPartialKey(short partialKey) {
        for(int i = 0 ; i < size ; ++i) {
            if(partialKeys[i] == partialKey) {
                return pointers[i];
            }
        }
        return null;
    }

    /**
     * 在查找不到时会调用insert函数，所以一定是嵌入TreeNode4类型
     */
    @Override
    public TreeNode insert(short partialKey) {
        partialKeys[size] = partialKey;
        TreeNode newNode = new TreeNode4();
        pointers[size] = newNode;
        size++;
        return newNode;
    }

    @Override
    public TreeNode insert(short partialKey, boolean isLeaf) {
        partialKeys[size] = partialKey;
        TreeNode newNode = new LeafNode256();
        pointers[size] = newNode;
        size++;
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
        for (int i = 0 ; i < size ; ++i) {
            if (partialKeys[i] == partialKey) {
                if (i == size - 1) {
                    partialKeys[i] = 0;
                    pointers[i] = null;
                } else {
                    // 遍历到size - 1，防止index超限
                    for (int j = i ; j < size - 1 ; ++j) {
                        partialKeys[j] = partialKeys[j + 1];
                        pointers[j] = pointers[j + 1];
                    }
                    partialKeys[size - 1] = 0;
                    pointers[size - 1] = null;
                }
            }
        }
        --size;
    }

    @Override
    public void update(short partialKey, TreeNode newNode) {
        for(int i = 0 ; i < size ; ++i) {
            if(partialKeys[i] == partialKey) {
                pointers[i] = newNode;
            }
        }
    }

    /**
     * 从node4扩展为node48
     */
    @Override
    public TreeNode grow() {
        // 注意size要预设为4,同时起始的赋值位置为1
        TreeNode48 treeNode48 = new TreeNode48((byte) 4);
        short right = 0;
        for (short partialKey : partialKeys) {
            short index = (short) (right + 1);
            treeNode48.partialKeys[partialKey] = index;
            treeNode48.pointers[index] = pointers[right];
            right++;
        }
        return treeNode48;
    }

    @Override
    public TreeNode shrink(TreeNode parentNode) {
        return null;
    }

    @Override
    public boolean isFull() {
        return size == 4;
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
