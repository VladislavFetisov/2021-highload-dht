package ru.mail.polis.lsm.vladislav_fetisov;

import ru.mail.polis.lsm.Record;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class Iterators {
    private Iterators() {

    }

    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        switch (iterators.size()) {
            case 0:
                return Collections.emptyIterator();
            case 1:
                return iterators.get(0);
            case 2:
                return mergeTwo(new PeekingIterator<>(iterators.get(0)), new PeekingIterator<>(iterators.get(1)));
            default:
                return mergeList(iterators);
        }
    }

    public static PeekingIterator<Record> mergeList(List<Iterator<Record>> iterators) {
        return iterators
                .stream()
                .map(PeekingIterator::new)
                .reduce(Iterators::mergeTwo)
                .orElse(new PeekingIterator<>(Collections.emptyIterator()));
    }

    private static PeekingIterator<Record> mergeTwo(PeekingIterator<Record> it1, PeekingIterator<Record> it2) {
        return new PeekingIterator<>(new Iterator<>() {

            @Override
            public boolean hasNext() {
                return it1.hasNext() || it2.hasNext();
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (!it1.hasNext()) {
                    return it2.next();
                }
                if (!it2.hasNext()) {
                    return it1.next();
                }
                Record record1 = it1.peek();
                Record record2 = it2.peek();
                int compare = record1.getKey().compareTo(record2.getKey());
                if (compare < 0) {
                    it1.next();
                    return record1;
                } else if (compare == 0) {
                    it1.next();
                    it2.next();
                    return record2;
                } else {
                    it2.next();

                    return record2;
                }
            }
        });
    }

    public static Iterator<Record> filteredResult(Iterators.PeekingIterator<Record> iterator) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                while (true) {
                    if (!iterator.hasNext()) {
                        return false;
                    }
                    Record record = iterator.peek();
                    if (!record.isTombstone()) {
                        return true;
                    }
                    iterator.next();
                }
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return iterator.next();
            }
        };
    }

    public static class PeekingIterator<T> implements Iterator<T> {
        private final Iterator<T> iterator;
        private T current = null;

        public PeekingIterator(Iterator<T> iterator) {
            this.iterator = iterator;
        }


        public T peek() {
            if (current == null) {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                current = iterator.next();
                return current;
            }
            return current;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext() || current != null;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            T res = peek();
            current = null;
            return res;
        }

    }
}
