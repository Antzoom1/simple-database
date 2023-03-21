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

    private File f;
    private TupleDesc td;
    private int id;
    private int pageSize;


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
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            int pageSize = BufferPool.getPageSize();
            byte[] pageData = new byte[pageSize];
            RandomAccessFile raf = new RandomAccessFile(this.f, "r");
            int offset = pageSize * pid.getPageNumber();
            // checks if the page isn't out of bounds
            if (offset + pageSize > raf.length()) {
                throw new IllegalArgumentException("page offset is resulting in a out of bounds");
            }
            raf.seek(offset);
            raf.read(pageData);
            // reads a random page of data and returns the page
            Page page = new HeapPage((HeapPageId) pid, pageData);
            return page;
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            // creates a raf
            RandomAccessFile write = new RandomAccessFile(getFile(), "rw");
            pageSize = BufferPool.getPageSize();
            byte[] pageData = page.getPageData();
            // conducts the write feature and closes the raf.
            write.seek((long) pageSize * page.getId().getPageNumber());
            write.write(pageData);
            write.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage page = null;
        int currentPageNumber = 0;
        // iterates through the current page number to insert tuple
        for (currentPageNumber = 0; currentPageNumber < numPages(); currentPageNumber++) {
            page = (HeapPage) Database.getBufferPool().getPage(tid,
                    new HeapPageId(getId(), currentPageNumber), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                break;
            }
        }
        // if no pages have space then will write out pages and then insert the tupl
        if (currentPageNumber == numPages()) {
            try {
                FileOutputStream write = new FileOutputStream(getFile(), true);
                write.write(new HeapPage(new HeapPageId(getId(), numPages()), new byte[BufferPool.getPageSize()]).getPageData());
                write.close();
                page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), currentPageNumber), Permissions.READ_WRITE);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException("Error in writing or reading the file");
            }
        }
        page.insertTuple(t);
        return new ArrayList<>(List.of(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // don't need to account if not enough pages and simply delete the tuple
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(List.of(page));

    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    // using the DbFileIterator interface to allow the HeapFile iterator to identify if there are empty
    // pages or not which will save a lot of space
    public class HeapFileIterator implements DbFileIterator {
        private final HeapFile heapFile;
        private final TransactionId transactionId;
        private int currPageNo;
        private Iterator<Tuple> iterator;

        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            heapFile = hf;
            transactionId = tid;
        }

        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open()
                throws DbException, TransactionAbortedException {
            currPageNo = 0;
            iterator = getTupleIterator(currPageNo);
        }

        /** @return true if there are more tuples available, false
         * if no more tuples or iterator isn't open. */
        public boolean hasNext()
                throws DbException, TransactionAbortedException {
            if (iterator == null) return false;

            // if no tuples are available, then return false
            while (!(iterator.hasNext())) {
                currPageNo++;
                if (currPageNo >= heapFile.numPages()) {
                    return false;
                }
                iterator = getTupleIterator(currPageNo);
            }

            return true;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next()
                throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!(hasNext())) {
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            currPageNo = 0;
            iterator = getTupleIterator(currPageNo);
        }

        /**
         * Closes the iterator.
         */
        public void close() {
            iterator = null;
        }

        // private helper method
        private Iterator<Tuple> getTupleIterator(int pageNo)
                throws NoSuchElementException, TransactionAbortedException, DbException {
            HeapPage tempPage = (HeapPage) Database.getBufferPool().getPage(
                    transactionId, new HeapPageId(heapFile.getId(), pageNo), Permissions.READ_ONLY);
            return tempPage.iterator();
        }
    }




}

