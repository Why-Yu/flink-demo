package flinkdemo.util;

import flinkdemo.entity.Node;
import flinkdemo.util.Interfaces.Heap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 实现的是基于Node的最小堆
 * @param <E>必须是Node或者Node的子类哦
 */
public class BinaryMinHeap<E extends Node> implements Heap<E> {

    private final List<E> minHeap;
    private final Class<E> type;

    /**
     * 构造函数可能看起来有些奇怪，但是minHeap的第一个位置必须是泛型参数的实例对象，但由于类型擦除,
     * 所以必须在构造函数中显示的传入Class对象，以便运行时能创建泛型参数的对象,填充第一个位置
     * @param type 类对象
     */
    public BinaryMinHeap(Class<E> type){
        this.type = type;
        minHeap = new ArrayList<>(64);
        minHeap.add(create(type));
    }

    public BinaryMinHeap(Class<E> type, int initialCapacity){
        this.type = type;
        minHeap = new ArrayList<>(initialCapacity);
        minHeap.add(create(type));
    }

    private E create(Class<E> type){
        try {
            return type.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //这样写是让swim支持同时输入Node或者int类型，同时让代码看起来更简洁
    @Override
    public void swim(E e) {
        int arrayIndex = e.arrayIndex;
        swim(arrayIndex);
    }

    public void swim(int i){
        int halfIndex = (int)Math.floor(i / 2);
        while(i > 1 && less(i, halfIndex)){
            exchange(i, halfIndex);
            i = halfIndex;
            halfIndex = (int)Math.floor(halfIndex / 2);
        }
    }

    @Override
    public void sink(E e) {
        int arrayIndex = e.arrayIndex;
        sink(arrayIndex);
    }

    public void sink(int i){
        while(2 * i <= minHeap.size() - 1){
            int doubleIndex = 2 * i;
            if(doubleIndex < minHeap.size() - 1 && less(doubleIndex + 1, doubleIndex)){
                doubleIndex++;
            }
            if(less(i, doubleIndex)) break;
            exchange(i, doubleIndex);
            i = doubleIndex;
        }
    }

    @Override
    public void exchange(int i, int j) {
        minHeap.get(i).arrayIndex = j;
        minHeap.get(j).arrayIndex = i;
        minHeap.set(i, minHeap.set(j, minHeap.get(i)));
    }
    @Override
    public boolean less(int i, int j) {
        return minHeap.get(i).compareTo(minHeap.get(j)) < 0;

    }

    @Override
    public boolean add(E e) {
        e.arrayIndex = minHeap.size();
        minHeap.add(e);
        swim(e.arrayIndex);
        return true;
    }

    @Override
    public E delMin() {
        exchange(1, minHeap.size() - 1);
        E minNode = minHeap.remove(minHeap.size() - 1);
        sink(1);
        return minNode;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        Iterator<E> iterator= minHeap.iterator();
        int i = 1;
        iterator.next();
        while(iterator.hasNext()){
            result.append(i++);
            result.append(":");
            result.append(iterator.next().total);
            result.append("   ");
        }
        return result.toString();
    }

    public E get(int i){
        return minHeap.get(i);
    }

    public int size() {
        return minHeap.size() - 1;
    }
}
