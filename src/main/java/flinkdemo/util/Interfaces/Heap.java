package flinkdemo.util.Interfaces;

public interface Heap<E> {
    /**
     * 元素的上浮
     * @param e 元素类型
     */
    void swim(E e);

    /**
     * 元素的下沉
     * @param e 元素类型
     */
    void sink(E e);

    void exchange(int i, int j);
    boolean less(int i, int j);
    boolean add(E e);
    E delMin();
}
