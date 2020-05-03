package simpledb;

import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private int afield;
    private Op what;
    protected HashMap<Field, Integer> gbHash;
    protected HashMap<Field, int[]> gbAvgHash;
    private Field nonGbKey;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.what = what;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.gbAvgHash = new HashMap<>();
        this.gbHash = new HashMap<>();
        this.nonGbKey = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field curGbField;
        IntField curAgField;
        int curCountVal;
        int curSumVal;

        if (gbfield == Aggregator.NO_GROUPING){
            //if (this.nonGbKey == null){
            //    this.nonGbKey = tup.getField(afield);
            //}
            curGbField = null;
        }
        else {
            curGbField = tup.getField(gbfield);
        }

        curAgField = (IntField) tup.getField(afield);
        int curVal = curAgField.getValue();

        switch (this.what){
            case MAX:
                if (!gbHash.containsKey(curGbField)){
                    gbHash.put(curGbField, curVal);
                } else {
                    int curMaxVal = gbHash.get(curGbField);
                    gbHash.put(curGbField, Math.max(curMaxVal, curVal));
                }
                return;

            case SUM:
                if (!gbHash.containsKey(curGbField)){
                    gbHash.put(curGbField, curVal);
                } else {
                    curSumVal = gbHash.get(curGbField);
                    gbHash.put(curGbField, curSumVal + curVal);
                }
                return;

            case COUNT:
                if (!gbHash.containsKey(curGbField)){
                    gbHash.put(curGbField, 1);
                } else {
                    curCountVal = gbHash.get(curGbField);
                    gbHash.put(curGbField, curCountVal + 1);
                }
                return;

            case MIN:
                if (!gbHash.containsKey(curGbField)){
                    gbHash.put(curGbField, curVal);
                } else {
                    int curMinVal = gbHash.get(curGbField);
                    gbHash.put(curGbField, Math.min(curMinVal,curVal));
                }
                return;

            case AVG:
                if (!gbAvgHash.containsKey(curGbField)){
                    gbAvgHash.put(curGbField, new int[]{curVal, 1});
                } else {
                    curSumVal = gbAvgHash.get(curGbField)[0];
                    curCountVal = gbAvgHash.get(curGbField)[1];
                    gbAvgHash.put(curGbField, new int[]{curSumVal + curVal, curCountVal + 1});
                }
                return;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
                    if (what != Op.AVG) {
                        curTuple.setField(0, new IntField(gbHash.get(nonGbKey)));
                    } else {
                        int avg = gbAvgHash.get(nonGbKey)[0] / gbAvgHash.get(nonGbKey)[1];
                        curTuple.setField(0, new IntField(avg));
                    }
                    agValArr[0] = curTuple;
                }
                else {
                    if (what != Op.AVG){
                        agValArr = new Tuple[gbHash.size()];
                        td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
                        int i = 0;
                        for (Field gbField: gbHash.keySet()){
                            Tuple curTuple = new Tuple(td);
                            curTuple.setField(0, gbField);
                            curTuple.setField(1, new IntField(gbHash.get(gbField)));
                            agValArr[i] = curTuple;
                            i++;
                        }
                    }else{
                        agValArr = new Tuple[gbAvgHash.size()];
                        td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
                        int i = 0;
                        for (Field gbField: gbAvgHash.keySet()){
                            Tuple curTuple = new Tuple(td);
                            curTuple.setField(0, gbField);
                            int avg = gbAvgHash.get(gbField)[0] / gbAvgHash.get(gbField)[1];
                            curTuple.setField(1, new IntField(avg));
                            agValArr[i] = curTuple;
                            i++;
                        }
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
    }
}
