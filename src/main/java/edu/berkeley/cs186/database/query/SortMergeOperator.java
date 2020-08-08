package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
         * Some member variables are provided for guidance, but there are many possible solutions.
         * You should implement the solution that's best for you, using any member variables you need.
         * You're free to use these member variables, but you're not obligated to.
         */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private final LeftRecordComparator leftRecordComparator = new LeftRecordComparator();
        private final RightRecordComparator rightRecordComparator = new RightRecordComparator();
        private final LeftRightRecordComparator recordComparator = new LeftRightRecordComparator();

        private BufferedWriter bf;

        private SortMergeIterator() {
            super();
            try {
                bf = new BufferedWriter(new FileWriter("output.log"));
                bf.write("");
            } catch (IOException e) {
                e.printStackTrace();
            }


            // input is not always sorted
            final SortOperator leftTable =
                new SortOperator(SortMergeOperator.this.getTransaction(), getLeftTableName(),
                    new LeftRecordComparator()),
                rightTable = new SortOperator(getTransaction(), getRightTableName(), new RightRecordComparator());
            String sortedLeftTable = leftTable.sort();
            String sortedRightTable = rightTable.sort();
            leftIterator = getRecordIterator(sortedLeftTable);
            rightIterator = getRecordIterator(sortedRightTable);
            leftIterator.markNext();
            rightIterator.markNext();
            leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
            fetchNextRecord();
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            Record nextRecord = this.nextRecord;
            fetchNextRecord();
            return nextRecord;
        }

        private int count = 0;

        private void fetchNextRecord() {
            log("\n\n");
            log("Fetch Next Record = " + count);
            nextRecord = null; // clear the flag
            if (leftRecord == null || rightRecord == null) {
                return;
            }
            while (nextRecord == null && leftRecord != null && rightRecord != null) {
                DataBox leftJoinValue = leftRecord.getValues().get(getLeftColumnIndex());
                DataBox rightJoinValue = rightRecord.getValues().get(getRightColumnIndex());
                log("Left Record = " + leftJoinValue + ", right record = " + rightJoinValue);
                final int cmp = leftJoinValue.compareTo(rightJoinValue);
                if (cmp == 0) {
                    final List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues()),
                        rightValues = new ArrayList<>(rightRecord.getValues());
                    leftValues.addAll(rightValues);
                    nextRecord = new Record(leftValues);
                    final DataBox compareJoinValue = leftJoinValue;
                    // move right
                    if (rightIterator.hasNext()) {
                        log("Moving Right " + count);
                        final Record prevRightRecord = rightRecord;
                        rightRecord = rightIterator.next();
                        assert rightRecord != null;
                        if (recordComparator.compare(leftRecord, rightRecord) != 0) {
                            // finished loop right, try to move left
                            if (leftIterator.hasNext()) {
                                log("Moving Left " + count);
                                leftRecord = leftIterator.next();

                                if (recordComparator.compare(leftRecord, prevRightRecord) == 0) {
                                    resetRight();
                                } else {
                                    rightIterator.markPrev();
                                }
                            } else {
                                log("End moving left " + count);
                                leftRecord = null; // every thing is looped
                                break;
                            }
                        }
                    } else {
                        // reset right, move left if possible
                        if (leftIterator.hasNext()) {
                            log("Moving Left " + count);
                            leftRecord = leftIterator.next();
                        } else {
                            leftRecord = null;
                        }
                        resetRight();
                    }
                } else if (cmp < 0) {
                    if (leftIterator.hasNext()) {
                        log("Moving Left " + count);
                        leftRecord = leftIterator.next();
                    } else {
                        // left become null, loop is over
                        leftRecord = null;
                        break;
                    }
                } else { // move right
                    if (rightIterator.hasNext()) {
                        log("Moving right");
                        final Record prev = rightRecord;
                        rightRecord = rightIterator.next();
                        if (rightRecordComparator.compare(rightRecord, prev) != 0) {
                            if (leftIterator.hasNext()) {
                                log("Moving Left " + count);
                                leftRecord = leftIterator.next();
                                resetRight();
                            } else {
                                leftRecord = null;
                                break;
                            }
                            rightIterator.markPrev(); // new Value
                        }
                    } else {
                        if (leftIterator.hasNext()) {
                            log("Moving Left " + count);
                            leftRecord = leftIterator.next();
                            log("reset right");
                            resetRight();
                        } else {
                            leftRecord = null;
                            break;
                        }
                    }
                }
            }

            count++;
        }

        private void resetRight() {
            log("Resetting right.");
            rightIterator.reset();
            rightRecord = rightIterator.next();
        }

        private void log(final String str) {
//                System.out.println("[" +count +"]" + str);
            try {
                bf.append("[" + count + "]" + str);
                bf.append("\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                    o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                    o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }

        private class LeftRightRecordComparator implements Comparator<Record> {

            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                    o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
