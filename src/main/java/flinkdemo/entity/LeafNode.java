package flinkdemo.entity;

import java.util.ArrayList;
import java.util.List;

public class LeafNode implements TreeNode{
    // 所处于的缓存路径ID
    private final List<Integer> pathID;
    // 在缓存路径中，该点所处的序号
    private final List<Integer> sequencePos;


    public LeafNode(int pathID, int sequencePos) {
        this.pathID = new ArrayList<>(3);
        this.sequencePos = new ArrayList<>(3);
        this.pathID.add(pathID);
        this.sequencePos.add(sequencePos);
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
        // pathID 和 sequencePos的索引是一一对应关系
        int index = this.pathID.indexOf(pathID);
        this.pathID.remove(index);
        this.sequencePos.remove(index);
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
    public List<Integer> getPathID() {
        return pathID;
    }

    @Override
    public List<Integer> getLeafValue() {
        return sequencePos;
    }
}
