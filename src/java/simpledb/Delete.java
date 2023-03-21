package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private TupleDesc tupleDesc;
    private boolean alreadyCalled;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.t = t;
        this.child = child;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        super.open();
        alreadyCalled = false;
    }

    public void close() {
        // some code goes here
        this.child.close();
        super.close();
        alreadyCalled = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        alreadyCalled = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (alreadyCalled) {
            return null;
        }
        alreadyCalled = true;
        int numDeleted = 0;
        while (child.hasNext()) {
            // keep trying to delete until there is no more child tuples
            Tuple tupleToDelete = child.next();
            try {
                Database.getBufferPool().deleteTuple(t, tupleToDelete);
            } catch (IOException | NoSuchElementException e){
                break;
            }
            numDeleted += 1;
        }
        Tuple countTuple = new Tuple(tupleDesc);
        // returns the count of tuples deleted
        countTuple.setField(0, new IntField(numDeleted));
        return countTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
