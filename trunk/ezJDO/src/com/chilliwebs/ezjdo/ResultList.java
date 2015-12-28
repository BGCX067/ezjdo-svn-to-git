/*
 * Copyright 2013 Nick Hecht chilliwebs@gmail.com.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chilliwebs.ezjdo;

import com.chilliwebs.ezjdo.exceptions.ezJDOException;
import java.util.*;

/**
 * @author Nick Hecht chilliwebs@gmail.com
 */
public class ResultList<T> implements List<T> {

    private List<T> resultlist;
    private final Results originalResults;

    
    public ResultList() {
        originalResults = null;
        resultlist = new ArrayList<T>();
    }
    
    /* package */ ResultList(Results originalResults) {
        this.originalResults = originalResults;
        resultlist = new ArrayList<T>();
        Iterator<T> iterator = originalResults.iterator();
        while (iterator.hasNext()) {
            resultlist.add(iterator.next());
        }
    }

    @Override
    public int size() {
        return resultlist.size();
    }

    public int total() throws ezJDOException {
        if (originalResults.pagedResults == null || originalResults.originalResults == null) {
            return size();
        } else {
            return originalResults.total();
        }
    }
    
    @Override
    public boolean isEmpty() {
        return resultlist.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
        return resultlist.iterator();
    }

    @Override
    public boolean contains(Object o) {
        return resultlist.contains(o);
    }

    @Override
    public Object[] toArray() {
        return resultlist.toArray();
    }

    @Override
    public <A> A[] toArray(A[] a) {
        return resultlist.toArray(a);
    }

    @Override
    public boolean add(T e) {
        return resultlist.add(e);
    }

    @Override
    public boolean remove(Object o) {
        return resultlist.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return resultlist.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return resultlist.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return resultlist.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return resultlist.retainAll(c);
    }

    @Override
    public void clear() {
        resultlist.clear();
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        return resultlist.addAll(index, c);
    }

    @Override
    public T get(int index) {
        return resultlist.get(index);
    }

    @Override
    public T set(int index, T element) {
        return resultlist.set(index, element);
    }

    @Override
    public void add(int index, T element) {
        resultlist.add(index, element);
    }

    @Override
    public T remove(int index) {
        return resultlist.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return resultlist.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return resultlist.lastIndexOf(o);
    }

    @Override
    public ListIterator<T> listIterator() {
        return resultlist.listIterator();
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        return resultlist.listIterator(index);
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        return resultlist.subList(fromIndex, toIndex);
    }
}
