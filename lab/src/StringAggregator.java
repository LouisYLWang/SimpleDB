package simpledb;

import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    protected HashMap<Field, Integer> gbFieldMap;
    private int aVal;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT){
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.gbFieldMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfield == Aggregator.NO_GROUPING){
            aVal += 1;
        } else {
            Field curField = tup.getField(gbfield);
            if (!gbFieldMap.containsKey(curField)){
                gbFieldMap.put(curField, 0);
            }
            int curVal = gbFieldMap.get(curField);
            gbFieldMap.put(curField, curVal+1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            private TupleDesc td;
            private Tuple[] agValArr;
            private int curIndex = 0;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (gbfield == Aggregator.NO_GROUPING) {
                    agValArr = new Tuple[1];
                    td = new TupleDesc(new Type[]{Type.INT_TYPE});
                    Tuple curTuple = new Tuple(td);
                    curTuple.setField(0, new IntField(aVal));
                    agValArr[0] = curTuple;
                } else {
                    agValArr = new Tuple[gbFieldMap.size()];
                    td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
                    int i = 0;
                    for (Field gbField: gbFieldMap.keySet()){
                        Tuple curTuple = new Tuple(td);
                        curTuple.setField(0, gbField);
                        curTuple.setField(1, new IntField(gbFieldMap.get(gbField)));
                        agValArr[i] = curTuple;
                        i++;
                    }
                }
        }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return curIndex < agValArr.length;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (hasNext()){
                    Tuple tuple = agValArr[curIndex++];
                    return tuple;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                curIndex = 0;
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                td = null;
                agValArr = null;
                curIndex = 0;
            }
        };
        //throw new UnsupportedOperationException("please implement me for lab2");
    }

}
