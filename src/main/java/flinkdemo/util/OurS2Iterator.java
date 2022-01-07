package flinkdemo.util;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2Iterator;
import com.google.common.geometry.S2Point;
import com.google.common.primitives.UnsignedLongs;
import com.google.common.geometry.S2ShapeIndex.CellRelation;

import java.util.ArrayList;
import java.util.List;

public final class  OurS2Iterator <T extends OurS2Iterator.Entry>{
    public interface Entry {
        /** Returns the cell ID of this cell as a primitive. */
        long id();
    }

    public static <T extends OurS2Iterator.Entry> OurS2Iterator<T> create(List<T> entries) {
        return new OurS2Iterator<T>(entries);
    }
    public final List<T> entries;
    public int pos;
    public int size;

    public OurS2Iterator() {
        this.entries = new ArrayList<>();
        this.pos = 0;
        this.size = 0;

    }

    public OurS2Iterator(List<T> entries) {
        this.entries = entries;
        this.size = entries.size();
    }

    public void seek(S2CellId target) {
        int start = 0;
        int end = size - 1;
        while (start <= end) {
            int mid = (start + end) / 2;
            long id = entries.get(mid).id();
            int result = UnsignedLongs.compare(id, target.id());
            if (result > 0) {
                end = mid - 1;
            } else if (result < 0) {
                start = mid + 1;
            } else if (start != mid) {
                end = mid;
            } else {
                pos = mid;
            }
        }
        pos = start;
    }

    /** Returns a copy of this iterator, positioned as this iterator is. */
    public OurS2Iterator<T> copy() {
        OurS2Iterator<T> it = new OurS2Iterator<T>(entries);
        it.pos = pos;
        return it;
    }

    /** Positions the iterator so that {@link #atBegin()} is true. */
    public void restart() {
        pos = 0;
    }

    /** Returns the comparison from the current iterator cell to the given cell ID. */
    public int compareTo(S2CellId cellId) {
        return UnsignedLongs.compare(entry().id(), cellId.id());
    }

    /** Returns true if {@code o} is an {@link S2Iterator} with equal entries and position. */
    @Override
    public boolean equals(Object o) {
        return o instanceof OurS2Iterator && equalIterators((OurS2Iterator<?>) o);
    }

    @Override
    public int hashCode() {
        return 31 * pos + entries.hashCode();
    }

    /** Returns true if these iterators have the same entries and position. */
    public <T extends OurS2Iterator.Entry> boolean equalIterators(OurS2Iterator<T> it) {
        return entries == it.entries && pos == it.pos;
    }
    /** Returns the cell id for the current cell. */
    public S2CellId id() {
        return new S2CellId(entry().id());
    }

    /** Returns the current entry. */
    public T entry() {
        // assert (!done());
        return entries.get(pos);
    }

    /** Returns the center of the cell (used as a reference point for shape interiors.) */
    public S2Point center() {
        return id().toPoint();
    }

    /**
     * Advances the iterator to the next cell in the index. Does not advance the iterator if {@code
     * pos} is equal to the number of cells in the index.
     */
    public void next() {
        if (pos < entries.size()) {
            pos++;
        }
    }

    /**
     * Positions the iterator at the previous cell in the index. Does not move the iterator if {@code
     * pos} is equal to 0.
     */
    public void prev() {
        if (pos > 0) {
            pos--;
        }
    }

    /** Returns true if the iterator is positioned past the last index cell. */
    public boolean done() {
        return pos == entries.size();
    }

    /** Returns true if the iterator is positioned at the first index cell. */
    public boolean atBegin() {
        return pos == 0;
    }

    /**
     * Advances the iterator to the next cell with id() >= target. If the iterator is {@link #done()}
     * or already satisfies id() >= target, there is no effect.
     */
    public void seekForward(S2CellId target) {
        if (!done() && compareTo(target) < 0) {
            int tmpPos = pos;
            seek(target);
            pos = Math.max(pos, tmpPos + 1);
        }
    }

    /** Positions the iterator so that {@link #done()} is true. */
    public void finish() {
        pos = entries.size();
    }

    public boolean locate(S2Point targetPoint) {
        // Let I be the first cell not less than T, where T is the leaf cell containing "targetPoint".
        // Then if T is contained by an index cell, then the containing cell is either I or I'. We
        // test for containment by comparing the ranges of leaf cells spanned by T, I, and I'.
        S2CellId target = S2CellId.fromPoint(targetPoint);
        seek(target);
        if (!done() && id().rangeMin().lessOrEquals(target)) {
            return true;
        }
        if (!atBegin()) {
            prev();
            if (id().rangeMax().greaterOrEquals(target)) {
                return true;
            }
        }
        return false;
    }

    public CellRelation locate(S2CellId target) {
        // Let T be the target, let I be the first cell not less than T.rangeMin(), and let I' be the
        // predecessor of I.  If T contains any index cells, then T contains I.  Similarly, if T is
        // contained by an index cell, then the containing cell is either I or I'.  We test for
        // containment by comparing the ranges of leaf cells spanned by T, I, and I'.
        seek(target.rangeMin());
        if (!done()) {
            if (id().greaterOrEquals(target) && id().rangeMin().lessOrEquals(target)) {
                return CellRelation.INDEXED;
            }
            if (id().lessOrEquals(target.rangeMax())) {
                return CellRelation.SUBDIVIDED;
            }
        }
        if (!atBegin()) {
            prev();
            if (id().rangeMax().greaterOrEquals(target)) {
                return CellRelation.INDEXED;
            }
        }
        return CellRelation.DISJOINT;
    }

    /** Set this iterator to the position given by the other iterator. */
    public void position(OurS2Iterator<T> it) {
        // assert index == it.index;
        pos = it.pos;
    }
}
