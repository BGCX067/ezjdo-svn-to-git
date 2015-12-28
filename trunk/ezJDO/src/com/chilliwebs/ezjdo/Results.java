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

import com.chilliwebs.ezjdo.exceptions.ezJDOValidationException;
import com.chilliwebs.ezjdo.exceptions.ezJDOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * @author Nick Hecht chilliwebs@gmail.com
 */
public class Results<T> implements Iterable<T> {

    /* package */ Results<T> originalResults;
    /* package */ Results<T> pagedResults;
    /* package */ Class<T> clazz;
    /* package */ Statement statement;
    private int updateCount = -1; // if the update count is -1 the count is not ready yet
    private int size = -1; // if the size is -1 the size is not ready yet
    private int tmpUpdateCount = 0;
    private int tableIndex = -1;
    private boolean iteratorTaken = false;
    private Integer pageNumber = -1;
    private Integer itemsPerPage = -1;
    /* package */ String originalSQL;
    /* package */ Object[] originalValues;
    /* package */ Object[] values;

    public Results() {
    }

    /* package */ Results(Results<T> originalResults, Integer pageNumber, Integer itemsPerPage) {
        this.originalResults = originalResults;
        SQLQueryHelper sqlQueryHelper;
        try {
            sqlQueryHelper = BaseObject.getSQLQueryHelper(originalResults.clazz.getName());
            if (sqlQueryHelper == null) {
                throw new RuntimeException(new ezJDOException("You must define a paging handler for the " + originalResults.clazz.getName() + " class before trying to page the results."));
            } else {
                this.pageNumber = pageNumber;
                this.itemsPerPage = itemsPerPage;
                pagedResults = sqlQueryHelper.createPagedResults(originalResults.clazz, originalSQL, originalValues, pageNumber, itemsPerPage);
            }
        } catch (ezJDOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /* package */ Results(Class<T> clazz, Statement statement, String originalSQL, Object... originalValues) {
        this.clazz = clazz;
        this.statement = statement;
        this.originalSQL = originalSQL;
        this.originalValues = originalValues;
        values = new Object[originalValues.length];
        System.arraycopy(originalValues, 0, values, 0, originalValues.length);
        init();
    }

    /* package */ Results(Class<T> clazz, Statement statement) {
        this.clazz = clazz;
        this.statement = statement;
        init();
    }

    private void init() {
        if (clazz == null) {
            try {
                updateCount = 0;
                if (statement instanceof CallableStatement) {
                    if (BaseObject.debugging) {
                    	BaseObject.log.log(Level.INFO, originalSQL);
                    }
                    //long start = System.currentTimeMillis();
                    if (!((CallableStatement) statement).execute()) {
                        tmpUpdateCount = this.statement.getUpdateCount();
                    }
                    //System.out.println(System.currentTimeMillis() - start);
                } else if (statement instanceof PreparedStatement) {
                    if (BaseObject.debugging) {
                        BaseObject.log.log(Level.INFO, originalSQL);
                    }
                    //long start = System.currentTimeMillis();
                    if (!((PreparedStatement) statement).execute()) {
                        tmpUpdateCount = this.statement.getUpdateCount();
                    }
                    //System.out.println(System.currentTimeMillis() - start);
                }
                do {
                    if (tmpUpdateCount != -1) {
                        updateCount += tmpUpdateCount;
                    }
                } while (!((this.statement.getMoreResults() == false) && ((tmpUpdateCount = this.statement.getUpdateCount()) == -1)));
                this.statement.close();
            } catch (SQLException ex) {
                throw new RuntimeException(new ezJDOException("The database threw and error while trying to generate the iterator. Please check your sql syntax.", ex));
            }
        } else {
            try {
                tableIndex = BaseObject.getTableCacheIndex(clazz.getName());
            } catch (ezJDOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    /* package */ final class ResultListIterator<T2> implements Iterator<T2> {

        private ResultSet resultSet;
        private boolean hasNext = false;
        private boolean hasKeys = false;
        private T2 object = null;
        private Class<T2> clazz;

        public ResultListIterator(Class<T2> clazz) throws ezJDOException {
            this.clazz = clazz;
            if (!iteratorTaken) {
                iteratorTaken = true;
                try {
                    if (clazz != null) {
                        if (statement instanceof CallableStatement) {
                            if (BaseObject.debugging) {
                                BaseObject.log.log(Level.INFO, originalSQL);
                            }
                            //long start = System.currentTimeMillis();
                            getNextResultset(((CallableStatement) statement).execute());
                            //System.out.println(System.currentTimeMillis() - start);
                        } else if (statement instanceof PreparedStatement) {
                            if (BaseObject.debugging) {
                                BaseObject.log.log(Level.INFO, originalSQL);
                            }
                            //long start = System.currentTimeMillis();
                            getNextResultset(((PreparedStatement) statement).execute());
                            //System.out.println(System.currentTimeMillis() - start);
                        }
                    }
                } catch (SQLException ex) {
                    throw new ezJDOException("The database threw and error while trying to generate the iterator. Please check your sql syntax.", ex);
                }
            } else {
                throw new ezJDOException("You cannot call the iterator once you have already called it.");
            }
        }

        private void getNextResultset(Boolean resultSetObject) throws SQLException {
            boolean hasMoreResults = true;
            int localUpdateCount = 0;
            while (hasMoreResults && !hasNext) {
                if (resultSetObject) {
                    hasNext = ((resultSet = statement.getResultSet()) != null && resultSet.next());
                } else {
                    localUpdateCount = statement.getUpdateCount();
                    if (localUpdateCount != -1) {
                        tmpUpdateCount += localUpdateCount;
                        if (statement instanceof CallableStatement) {
                            hasNext = false;
                        } else if (statement instanceof PreparedStatement) {
                            hasKeys = hasNext = ((resultSet = ((PreparedStatement) statement).getGeneratedKeys()) != null && !resultSet.isClosed() && resultSet.next() && resultSet.getObject(1) != null);
                        }
                    }
                }
                if (!hasNext) {
                    hasMoreResults = !(((resultSetObject = statement.getMoreResults()) == false) && (localUpdateCount == -1));
                }
            }
            if (!hasMoreResults) {
                updateCount = tmpUpdateCount;
                statement.close();
            }
        }

        @Override
        public boolean hasNext() {
            if (clazz != null && !hasNext) {
                hasKeys = false;
                try {
                    if (!statement.isClosed()) {
                        getNextResultset(statement.getMoreResults());
                    }
                } catch (SQLException ex) {
                    throw new RuntimeException(new ezJDOException("The database threw and error while trying to fetch more results. Please check your database connection.", ex));
                }
            }
            return hasNext;
        }

        @Override
        public T2 next() {
            try {
                object = null;
                if (hasNext) {
                    if (hasKeys) {
                        if (BaseObject.class.isAssignableFrom(clazz)) {
                            Object[] keys = new Object[BaseObject.keyPos[tableIndex].length];
                            for (int i = 0; i < keys.length; i++) {
                                keys[i] = resultSet.getObject(i + 1);
                            }
                            object = clazz.cast(BaseObject.find(clazz, keys));
                        } else if (clazz.isArray()) {
                            int size = resultSet.getMetaData().getColumnCount();
                            object = clazz.cast(Array.newInstance(clazz.getComponentType(), size));
                            for (int i = 0; i < size; i++) {
                                ((Object[]) object)[i] = resultSet.getObject(i + 1);
                            }
                        } else {
                            Object var = resultSet.getObject(1);
                            if (var.getClass() == BigDecimal.class && Integer.class.isAssignableFrom(clazz)) { // error with jdbc keys as bigint insted of integer
                                var = ((BigDecimal) var).intValue();
                            }
                            object = clazz.cast(var);
                        }
                    } else {
                        object = clazz.cast(BaseObject.construct(clazz, resultSet));
                    }
                    hasNext = resultSet.next();
                    if (!hasNext) {
                        resultSet.close();
                    }
                }
            } catch (ezJDOException ex) {
                throw new RuntimeException(ex);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            return object;
        }

        @Override
        public void remove() {
            if (BaseObject.class.isAssignableFrom(clazz)) {
                try {
                    ((BaseObject) object).delete();
                } catch (ezJDOValidationException ex) {
                    throw new RuntimeException(ex);
                } catch (ezJDOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    @Override
    public Iterator<T> iterator() {
        if (pagedResults == null) {
            try {
                return new ResultListIterator<T>(clazz);
            } catch (ezJDOException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            return pagedResults.iterator();
        }
    }

    public int size() throws ezJDOException {
        if (size == -1) {
            if (pagedResults == null) {
                Pattern p = Pattern.compile("(group|order)\\s+by.*", Pattern.CASE_INSENSITIVE);
                String SQL = p.matcher(originalSQL).replaceAll("");
                String countSQL = "SELECT COUNT(*) FROM (" + SQL + ") ezcount";
                try {
                    PreparedStatement prepStmt = BaseObject.getConnection(BaseObject.getTableCacheIndex(clazz.getName()), Thread.currentThread().getId()).prepareStatement(countSQL);
                    int i = 1;
                    for (Object value : values) {
                        prepStmt.setObject(i++, value);
                    }
                    if (BaseObject.debugging) {
                        BaseObject.log.log(Level.INFO, countSQL);
                    }
                    //long start = System.currentTimeMillis();
                    ResultSet rs = prepStmt.executeQuery();
                    //System.out.println(System.currentTimeMillis() - start);
                    rs.next();
                    size = rs.getInt(1);
                } catch (SQLException ex) {
                    throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
                }
            } else {
                int totalItems = total();
                if (itemsPerPage == -1 || pageNumber == -1) {
                    size = totalItems;
                }
                int lastPage = (totalItems / itemsPerPage) + ((totalItems % itemsPerPage > 0) ? 1 : 0);
                if (pageNumber > lastPage) {
                    size = 0;
                } else if (pageNumber == lastPage) {
                    size = totalItems % itemsPerPage;
                } else {
                    size = itemsPerPage;
                }
            }
        }
        return size;
    }

    public int total() throws ezJDOException {
        if (pagedResults == null || originalResults == null) {
            return size();
        } else {
            return originalResults.size();
        }
    }

    public boolean isEmpty() throws ezJDOException {
        return size() == 0;
    }
    private ResultList<T> list = null;

    public ResultList<T> list() {
        if (list == null) {
            list = new ResultList<T>(this);
        }
        return list;
    }

    public Results<T> paged(Integer pageNumber, Integer itemsPerPage) {
        return new Results<T>(this, pageNumber, itemsPerPage);
    }

    public int getUpdateCount() {
        return updateCount;
    }
}
