package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private int numFields;
    private TDItem[] TDAr;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public Type getFieldType() {
            return fieldType;
        }

        public String getFieldName() {
            return fieldName;
        }

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        Iterator<TDItem> it = new Iterator<TDItem>() {
            private int curIndex = 0;

            @Override
            public boolean hasNext() {
                return curIndex < numFields;
            }

            @Override
            public TDItem next() {
                return TDAr[curIndex++];
            }
        };
        return it;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        assert typeAr.length > 0;
        if (fieldAr == null){
            fieldAr = new String[typeAr.length];
        } else if (fieldAr.length != typeAr.length){
            fieldAr = Arrays.copyOf(fieldAr, typeAr.length);
        }
        TDAr = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; ++i){
            TDAr[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
        this.numFields = typeAr.length;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, null);
    }

    public TupleDesc(TDItem[] tdAr) {
        assert tdAr.length > 0;
        this.TDAr = tdAr;
        this.numFields = tdAr.length;
    }


    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= this.numFields){
            throw new NoSuchElementException();
        }
        return this.TDAr[i].getFieldName();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= this.numFields){
            throw new NoSuchElementException();
        }
        return this.TDAr[i].getFieldType();
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        // what if duplicate name? only return first appear doesn't make sense
        for(int i = 0; i < this.numFields; i++){
            String curFN = this.TDAr[i].getFieldName();
            if (curFN != null && curFN.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int tuSize = 0;
        for (int i = 0; i < this.numFields; i++){
            tuSize += this.getFieldType(i).getLen();
        }
        return tuSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int ntd1 = td1.numFields();
        int ntd2 = td2.numFields();
        int newNumFields = td1.numFields() + td2.numFields();

        TDItem[] newTDAr = new TDItem[newNumFields];
        System.arraycopy(td1.TDAr, 0, newTDAr,0, ntd1);
        System.arraycopy(td2.TDAr, 0, newTDAr,ntd1, ntd2);

        return new TupleDesc(newTDAr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TupleDesc)) return false;
        TupleDesc tupleDesc = (TupleDesc) o;
        return Arrays.deepToString(TDAr).equals(Arrays.deepToString(tupleDesc.TDAr));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(TDAr);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String[] s = new String[this.numFields()];
        for (int i = 0; i < this.numFields(); i ++){
            s[i] = TDAr[i].toString();
        }
        return String.join(",", s);
    }
}
