package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private HashMap<Integer, IntHistogram> intFieldsHistograms;
    private HashMap<Integer, StringHistogram> stringFieldsHistograms;
    private HashMap<Integer, HashSet<Integer>> intFieldsValMemo;
    private HashMap<Integer, HashSet<String>> stringFieldsValMemo;
    private int ioCostPerPage;
    private int tuplesNum;
    private int pgNum;
    private TupleDesc tp;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        DbFileIterator fileIterator = dbFile.iterator(new TransactionId());

        try {
            fileIterator.open();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }

        this.tuplesNum = 0;
        this.pgNum = ((HeapFile)dbFile).numPages();
        this.ioCostPerPage = ioCostPerPage;
        this.intFieldsHistograms = new HashMap<>();
        this.stringFieldsHistograms = new HashMap<>();
        this.intFieldsValMemo = new HashMap<>();
        this.stringFieldsValMemo = new HashMap<>();
        this.tp = dbFile.getTupleDesc();

        HashMap<Integer, Integer> intFieldMax = new HashMap<>();
        HashMap<Integer, Integer> intFieldMin = new HashMap<>();

        try {
            while(fileIterator.hasNext()){
                Tuple t = fileIterator.next();
                this.tuplesNum++;
                for (int i = 0; i < t.numFields(); i ++){
                    Field curField = t.getField(i);
                    if (curField.getType() == Type.INT_TYPE ){
                        IntField curIntField = (IntField) t.getField(i);
                        int curFieldVal = curIntField.getValue();
                        intFieldMin.putIfAbsent(i, curFieldVal);
                        intFieldMax.putIfAbsent(i, curFieldVal);
                        intFieldMin.replace(i, Math.min(intFieldMin.get(i), curFieldVal));
                        intFieldMax.replace(i, Math.max(intFieldMax.get(i), curFieldVal));
                    }
                }
            }
            fileIterator.rewind();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }

        try {
            while(fileIterator.hasNext()){
                Tuple t = fileIterator.next();
                for (int i = 0; i < t.numFields(); i ++){
                    Field curField = t.getField(i);
                    if (curField.getType() == Type.INT_TYPE ){
                        int curFieldVal = ((IntField) t.getField(i)).getValue();
                        intFieldsValMemo.putIfAbsent(i, new HashSet<>());
                        intFieldsValMemo.get(i).add(curFieldVal);
                        this.intFieldsHistograms.putIfAbsent(i, new IntHistogram(NUM_HIST_BINS, intFieldMin.get(i), intFieldMax.get(i)));
                        IntHistogram curHist = this.intFieldsHistograms.get(i);
                        curHist.addValue(curFieldVal);

                    }  else if (curField.getType() == Type.STRING_TYPE ){

                        String curFieldVal = ((StringField) t.getField(i)).getValue();
                        stringFieldsValMemo.putIfAbsent(i, new HashSet<>());
                        stringFieldsValMemo.get(i).add(curFieldVal);
                        this.stringFieldsHistograms.putIfAbsent(i, new StringHistogram(NUM_HIST_BINS));
                        StringHistogram curHist = this.stringFieldsHistograms.get(i);
                        curHist.addValue(curFieldVal);
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        } finally {
            fileIterator.close();
        }
    }


    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.pgNum * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (selectivityFactor * this.tuplesNum);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (constant.getType() == Type.INT_TYPE){
            if (!this.intFieldsHistograms.containsKey(field)){
                return -1.0;
            }
            return this.intFieldsHistograms.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        }
        if (constant.getType() == Type.STRING_TYPE){
            if (!this.stringFieldsHistograms.containsKey(field)){
                return -1.0;
            }
            return this.stringFieldsHistograms.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        }
        return -1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.tuplesNum;
    }



    /**
     * return the total number of distinct val of given columns in this table
     * */
    public int getDistinctVal(int field) {
        if (this.tp.getFieldType(field) == Type.INT_TYPE) {
            return this.intFieldsValMemo.get(field).size();
        } else if (this.tp.getFieldType(field) == Type.STRING_TYPE) {
            return this.stringFieldsValMemo.get(field).size();
        }
        return -1;
    }
}
