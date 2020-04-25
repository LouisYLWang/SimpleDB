package simpledb;

import javax.xml.crypto.Data;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private boolean openStatus;
    private DbFile dbfile;
    private DbFileIterator it;
    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.openStatus = false;
        this.dbfile = Database.getCatalog().getDatabaseFile(this.tableid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.it = dbfile.iterator(tid);
        it.open();
        this.openStatus = true;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc oriTupleDesc = this.dbfile.getTupleDesc();
        int numField = oriTupleDesc.numFields();
        TupleDesc.TDItem[] tdItems = new TupleDesc.TDItem[numField];
        int i = 0;
        for (Iterator<TupleDesc.TDItem> iter = oriTupleDesc.iterator(); iter.hasNext();) {
            TupleDesc.TDItem tdItem = iter.next();
            Type t = tdItem.fieldType;
            String n = this.tableAlias + "." + tdItem.fieldName;
            tdItems[i] = new TupleDesc.TDItem(t,n);
            i++;
        }
        return new TupleDesc(tdItems);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.openStatus){
            return this.it.hasNext();
        }
        return false;
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (this.openStatus) {
            return this.it.next();
        }
        return null;
    }

    public void close() {
        // some code goes here
        this.openStatus = false;
        it.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        it.rewind();
    }
}
