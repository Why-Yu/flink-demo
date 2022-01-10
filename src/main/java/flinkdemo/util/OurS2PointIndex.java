package flinkdemo.util;

import com.google.common.annotations.GwtCompatible;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2ClosestPointQuery;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import com.google.common.primitives.UnsignedLongs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * S2PointIndex maintains an index of points sorted by leaf S2CellId. Each point has some associated
 * client-supplied data, such as an index or object the point was taken from, useful to map query
 * results back to another data structure.
 *
 * <p>The class supports adding or removing points dynamically, and provides a seekable iterator
 * interface for navigating the index.
 *
 * <p>You can use this class in conjunction with {@link S2ClosestPointQuery} to find the closest
 * index points to a given query point. For example:
 *
 * <pre>
 * void test(List<S2Point> points, S2Point target) {
 *   // The generic type allows auxiliary data to be attached to each point
 *   // In this case, attach the original index of the point.
 *   S2PointIndex<Integer> index = new S2PointIndex();
 *   for (int i = 0; i < points.size(); i++) {
 *     index.add(points.get(i), i);
 *   }
 *   S2ClosestPointQuery<Integer> query = new S2ClosestPointQuery<>(index);
 *   query.findClosestPoint(target);
 *   if (query.num_points() > 0) {
 *     // query.point(0) is the closest point (result 0).
 *     // query.distance(0) is the distance to the target.
 *     // query.data(0) is the auxiliary data (the array index set above).
 *     doSomething(query.point(0), query.data(0), query.distance(0));
 *   }
 * }
 * </pre>
 *
 * <p>Alternatively, you can access the index directly using the iterator interface. For example,
 * here is how to iterate through all the points in a given S2CellId "targetId":
 *
 * <pre>
 * S2Iterator<S2PointIndex.Entry<Integer>> it = index.iterator();
 * it.seek(targetId.rangeMin());
 * for (; !it.done() && it.compareTo(targetId.rangeMax()) <= 0; it.next()) {
 *   doSomething(it.entry());
 * }
 * </pre>
 *
 * <p>Points can be added or removed from the index at any time by calling add() or remove(), but
 * doing so invalidates existing iterators. New iterators must be created.
 *
 * <p>This class is not thread-safe.
 */
// TODO(user): Make this a subtype of S2Region, so that it can also be used to efficiently compute
// coverings of a collection of S2Points.
@GwtCompatible
public final class OurS2PointIndex<Data> {
    public final ArrayList<Entry<Data>> entries = Lists.newArrayList();
    public boolean sorted = true;

    /** Returns the number of points in the index. */
    public int numPoints() {
        return entries.size();
    }

    /**
     * Returns a new iterator over the cells of this index, after sorting entries by cell ID if any
     * modifications have been made since the last iterator was created.
     */
    public OurS2Iterator<Entry<Data>> iterator() {
        if (!sorted) {
            Collections.sort(entries);
            sorted = true;
        }
        return OurS2Iterator.create(entries);
    }

    /** As {@link #add(Entry)}, but more convenient. */
    public void add(S2Point point, Data data) {
        add(createEntry(point, data));
    }

    /** Adds a new entry to the index. Invalidates all iterators; clients must create new ones. */
    public void add(Entry<Data> entry) {
        sorted = false;
        entries.add(entry);
    }

    public void addAll(List<S2Point> pointList, List<Data> dataList) {
        ArrayList<Entry<Data>> addList = Lists.newArrayList();
        int index = 0;
        for (S2Point point : pointList) {
            addList.add(createEntry(point, dataList.get(index)));
            ++index;
        }
        sorted = false;
        entries.addAll(addList);
    }

    /** As {@link #remove(Entry)}, but more convenient. */
    public boolean remove(S2Point point, Data data) {
        return remove(createEntry(point, data));
    }

    /**
     * Removes the given entry from the index, and returns whether the given entry was present and
     * removed. Both the "point" and "data" fields must match the point to be removed. Invalidates all
     * iterators; clients must create new ones.
     */
    public boolean remove(Entry<Data> entry) {
        return entries.remove(entry);
    }

    public void removeAll(List<String> pathSequence, List<Data> dataList) {
        ArrayList<Entry<Data>> removeList = Lists.newArrayList();
        int index = 0;
        for (String dataIndex : pathSequence) {
            S2Point point = TopologyGraph.getVertex(dataIndex);
            removeList.add(createEntry(point, dataList.get(index)));
        }
        entries.removeAll(removeList);
    }

    /**
     * Resets the index to its original empty state. Invalidates all iterators; clients must create
     * new ones.
     */
    public void reset() {
        sorted = true;
        entries.clear();
    }

    /** Convenience method to create an index entry from the given point and data value. */
    public static <Data> Entry<Data> createEntry(S2Point point, Data data) {
        return new Entry<>(S2CellId.fromPoint(point), point, data);
    }

    /**
     * An S2Iterator-compatible pair of S2Point with associated client data of a given type.
     *
     * <p>Equality and hashing are based on the point and data value. The natural order of this type
     * is by the leaf cell that contains the point, which is <strong>not</strong> consistent with
     * equals.
     */
    public static class Entry<Data> implements OurS2Iterator.Entry, Comparable<Entry<Data>> {
        public final long id;
        public final S2Point point;
        public final Data data;

        public Entry(S2CellId cellId, S2Point point, Data data) {
            this.id = cellId.id();
            this.point = point;
            this.data = data;
        }

        public Entry() {
            this.id = 0;
            this.point = new S2Point();
            this.data = null;
        }

        @Override
        public long id() {
            return id;
        }

        public S2Point point() {
            return point;
        }

        public Data data() {
            return data;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof Entry) {
                Entry<?> e = (Entry<?>) other;
                return point.equalsPoint(e.point) && Objects.equal(data, e.data);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return point.hashCode() * 31 + (data == null ? 0 : data.hashCode());
        }

        @Override
        public int compareTo(Entry<Data> other) {
            return UnsignedLongs.compare(id, other.id);
        }

        @Override
        public String toString() {
            return new S2LatLng(point) + ": " + data;
        }
    }
}

