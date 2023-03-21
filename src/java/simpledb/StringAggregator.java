package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Field, Integer> gbfieldMap;

    private int aValue;
    private TupleDesc tupleDesc;
    Field fieldKey;
    List<Tuple> tuples = new ArrayList<Tuple>();



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
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.gbfieldMap = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // sets the tupleDesc to the new Tuple
        tupleDesc = tup.getTupleDesc();
        // if there is a field, we set the fieldKey to the tuple Field
        if (gbfield != NO_GROUPING) {
            fieldKey = tup.getField(gbfield);
        } else {
            fieldKey = null;
        }
        // updating the aggregator
        if (!(gbfieldMap.containsKey(fieldKey))) {
            gbfieldMap.put(fieldKey, 1);
        } else {
            gbfieldMap.put(fieldKey, gbfieldMap.get(fieldKey) + 1);
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
        // if there is a grouping, we group by groupValue and aggregateValue
        if (gbfield != NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                    new String[]{tupleDesc.getFieldName(gbfield), tupleDesc.getFieldName(afield)});
            // we then iterate through the fieldValues map
            for (Field fieldKey : gbfieldMap.keySet()) {
                Tuple tuple = new Tuple(tupleDesc);
                // we then set the fields and add it to the Tuple arraylist
                tuple.setField(0, fieldKey);
                tuple.setField(1, new IntField(gbfieldMap.get(fieldKey)));
                tuples.add(tuple);
            }
        } else {  // otherwise we just group by the aggregateValue
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
            // we then iterate through the fieldValues map
            for (Field fieldKey : gbfieldMap.keySet()) {
                Tuple tuple = new Tuple(tupleDesc);
                // we then create and set a new field and add it to the Tuple arraylist
                tuple.setField(0, new IntField(gbfieldMap.get(fieldKey)));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc, tuples);
    }
}
