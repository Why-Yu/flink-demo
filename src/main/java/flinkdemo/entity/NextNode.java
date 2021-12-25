package flinkdemo.entity;

public class NextNode {
    public String dataIndex;
    public double weight;
    public NextNode nextNode;

    public NextNode() {

    }

    public NextNode(String dataIndex, double weight){
        this.dataIndex = dataIndex;
        this.weight = weight;
    }

    public String getDataIndex() {
        return dataIndex;
    }

    public void setDataIndex(String dataIndex) {
        this.dataIndex = dataIndex;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public NextNode getNextNode() {
        return nextNode;
    }

    public void setNextNode(NextNode nextNode) {
        this.nextNode = nextNode;
    }

    @Override
    public String toString() {
        return "NextNode{" +
                "dataIndex=" + dataIndex +
                ", weight=" + weight +
                ", nextNode=" + nextNode +
                '}';
    }
}
