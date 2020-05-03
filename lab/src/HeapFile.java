package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private final File f;
    private final TupleDesc td;
    private final int id;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc(){
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        int pgSIZE = BufferPool.getPageSize();
        byte data[];
        try{
            data = new byte[pgSIZE];
            RandomAccessFile raf = new RandomAccessFile(this.f, "r");
            int offset = pid.getPageNumber() * pgSIZE;
            if (offset + BufferPool.getPageSize() > raf.length()){
                throw new IllegalArgumentException("page offset too long");
            }
            raf.seek(offset);
            raf.read(data);
            Page page = new HeapPage((HeapPageId) pid, data);
            return page;
        } catch (IllegalArgumentException | IOException e){
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
        RandomAccessFile raf = new RandomAccessFile(this.f, "rw");
        raf.seek(offset);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.f.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifiedPagesArr = new ArrayList<>();

        for (int i = 0; i < numPages(); i++){
            PageId pid = new HeapPageId(getId(),i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if(page.getNumEmptySlots()>0){
                page.insertTuple(t);
                modifiedPagesArr.add(page);
                break;
            }
        }
        if (modifiedPagesArr.isEmpty()){
            int pgSIZE = BufferPool.getPageSize();
            HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), new byte[pgSIZE]);
            newPage.insertTuple(t);
            this.writePage(newPage);
            modifiedPagesArr.add(newPage);
        }
        return modifiedPagesArr;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifiedPagesArr = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        page.deleteTuple(t);
        modifiedPagesArr.add(page);
        return modifiedPagesArr;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid)  {
        // some code goes her
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        private int curPgNum = 0;
        private HeapPageId pid;
        private HeapPage curPg;
        private Iterator<Tuple> it;
        private TransactionId tid;
        private boolean statusOpen;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.statusOpen = false;
        }

        public void openByPgNum(int pgNum) throws DbException, TransactionAbortedException {
            this.curPgNum = pgNum;
            this.pid = new HeapPageId(getId(), curPgNum);
            this.curPg = (HeapPage) Database.getBufferPool().getPage(this.tid,
                    pid, Permissions.READ_ONLY);
            this.it = curPg.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            openByPgNum(0);
            this.statusOpen = true;
        }

        @Override

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.statusOpen){
                if (this.it.hasNext())
                    return true;
                while (this.curPgNum + 1 < numPages()) {
                    openByPgNum(this.curPgNum + 1);
                    if (this.it.hasNext())
                        return true;
                }
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()){
                return it.next();
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            this.curPgNum = 0;
            this.pid = null;
            this.curPg = null;
            this.it = null;
            this.statusOpen = false;
        }
    }
}

