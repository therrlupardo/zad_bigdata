class ComparablePair<A extends Comparable<? super A>, B extends Comparable<? super B>> extends javafx.util.Pair<A, B>
        implements Comparable<ComparablePair<A, B>> {

    public ComparablePair(A key, B value) {
        super(key, value);
    }

    @Override
    public int compareTo(ComparablePair<A, B> o) {
        int cmp = o == null ? 1 : (this.getKey()).compareTo(o.getKey());
        return cmp == 0 ? (this.getValue()).compareTo(o.getValue()) : cmp;
    }

}