package flinkdemo.entity;

public class LeafNode implements TreeNode{
    // 所处于的缓存路径ID
    private final int pathID;
    // 在缓存路径中，该点所处的序号
    private final int sequencePos;


    public LeafNode(int pathID, int sequencePos) {
        this.pathID = pathID;
        this.sequencePos = sequencePos;
    }

    @Override
    public TreeNode getByPartialKey(short partialKey) {
        return null;
    }

    @Override
    public TreeNode insert(short partialKey) {
        return null;
    }

    @Override
    public TreeNode insert(short partialKey, boolean isLeaf) {
        return null;
    }

    @Override
    public TreeNode insert(short partialKey, int pathID, int sequencePos) {
        return null;
    }

    @Override
    public void delete(short partialKey, int pathID) {

    }

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
        return false;
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public int getPathID() {
        return pathID;
    }

    @Override
    public int getLeafValue() {
        return sequencePos;
    }
}
