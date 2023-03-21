package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Field, Integer> gbfieldVal;
    private ConcurrentHashMap<Field, Integer> gbfieldCounts;
    private TupleDesc tupleDesc;
    private Field fieldKey;
    private int tupleValue;
    List<Tuple> tuples = new ArrayList<Tuple>();
    TupleDesc newTupleDesc;
    private String gbfieldName;

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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        // initializes on ConcurrencyHashMap to the aggregate counts and values
        this.gbfieldCounts = new ConcurrentHashMap<Field, Integer>();
        this.gbfieldVal = new ConcurrentHashMap<Field, Integer>();

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
        tupleDesc = tup.getTupleDesc();
        if (this.gbfield != NO_GROUPING) {
            // sets the fieldKey to the merging Tuple
            fieldKey = tup.getField(gbfield);
        }
        tupleValue = ((IntField) tup.getField(afield)).getValue();
        // goes through the cases based on the Op operator
        switch (what) {
            case COUNT:
                if (!(gbfieldVal.containsKey(fieldKey))) {
                    gbfieldVal.put(fieldKey, 1);
                } else {
                    gbfieldVal.put(fieldKey, gbfieldVal.get(fieldKey) + 1);
                }
                break;
            case MAX:
                if (!(gbfieldVal.containsKey(fieldKey))) {
                    gbfieldVal.put(fieldKey, tupleValue);
                } else {
                    gbfieldVal.put(fieldKey, Math.max(gbfieldVal.get(fieldKey), tupleValue));
                }
                break;
            case MIN:
                if (!(gbfieldVal.containsKey(fieldKey))) {
                    gbfieldVal.put(fieldKey, tupleValue);
                } else {
                    gbfieldVal.put(fieldKey, Math.min(gbfieldVal.get(fieldKey), tupleValue));
                }
                break;
            case SUM:
                if (!(gbfieldVal.containsKey(fieldKey))) {
                    gbfieldVal.put(fieldKey, tupleValue);
                } else {
                    gbfieldVal.put(fieldKey, gbfieldVal.get(fieldKey) + tupleValue);
                }
                break;
            case AVG:
                if (!(gbfieldVal.containsKey(fieldKey))) {
                    gbfieldCounts.put(fieldKey, 1);
                    gbfieldVal.put(fieldKey, tupleValue);
                } else {
                    gbfieldVal.put(fieldKey, gbfieldVal.get(fieldKey) + tupleValue);
                    gbfieldCounts.put(fieldKey, gbfieldCounts.get(fieldKey) + 1);
                }
                break;
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
        // I used the TupleIterator that implements the OpIterator
        if (gbfield != NO_GROUPING) {
            // if grouped then we group by the groupValue and the aggregateValue
            newTupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                    new String[]{tupleDesc.getFieldName(gbfield),
                            tupleDesc.getFieldName(afield)});
            for (Field gbfieldKey : gbfieldVal.keySet()) {
                int aggregateVal = gbfieldVal.get(gbfieldKey);
                // the case if the Op was computing the average
                if (what == Op.AVG) {
                    aggregateVal = (aggregateVal / gbfieldCounts.get(gbfieldKey));
                }
                Tuple tuple = new Tuple(newTupleDesc);
                // sets the field type and the groupValue and adds it to the arraylist
                // of tuples.
                tuple.setField(0, gbfieldKey);
                tuple.setField(1, new IntField(aggregateVal));
                tuples.add(tuple);
            }
        } else {
            // the case where there is no grouping
            newTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
            // we traverse through the the grouped values and aggregate
            for (Field fieldKey : gbfieldVal.keySet()) {
                int aggregateVal = gbfieldVal.get(fieldKey);
                // the case if the Op was computing the average
                if (what == Op.AVG) {
                    aggregateVal = (aggregateVal / gbfieldCounts.get(fieldKey));
                }
                Tuple tuple = new Tuple(newTupleDesc);
                tuples.add(tuple);
                // adds and set the fields of the aggregateValues
                tuple.setField(0, new IntField(aggregateVal));
            }
        }
        return new TupleIterator(newTupleDesc, tuples);
    }
}
