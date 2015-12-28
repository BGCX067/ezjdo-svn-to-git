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

import com.chilliwebs.ezjdo.annotations.And;
import com.chilliwebs.ezjdo.annotations.BaseConfig;
import com.chilliwebs.ezjdo.annotations.Column;
import com.chilliwebs.ezjdo.annotations.Key;
import com.chilliwebs.ezjdo.annotations.Many;
import com.chilliwebs.ezjdo.exceptions.ezJDOException;
import com.chilliwebs.ezjdo.exceptions.ezJDOValidationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.bind.annotation.XmlTransient;

/**
 * @author Nick Hecht chilliwebs@gmail.com
 */
@XmlTransient
public class BaseObject {

    private static final Map<String, String> JDBC_JAVA_TYPES;

    static {
        Map<String, String> m = new LinkedHashMap<String, String>();

        m.put("VARCHAR", String.class.getCanonicalName());
        m.put("CHAR", String.class.getCanonicalName());
        m.put("LONGVARCHAR", String.class.getCanonicalName());
        m.put("DECIMAL", java.math.BigDecimal.class.getCanonicalName());
        m.put("NUMERIC", java.math.BigDecimal.class.getCanonicalName());
        m.put("BOOLEAN", boolean.class.getCanonicalName());
        m.put("BIT", boolean.class.getCanonicalName());
        m.put("TINYINT", byte.class.getCanonicalName());
        m.put("SMALLINT", short.class.getCanonicalName());
        m.put("INTEGER", int.class.getCanonicalName());
        m.put("BIGINT", long.class.getCanonicalName());
        m.put("REAL", float.class.getCanonicalName());
        m.put("DOUBLE", double.class.getCanonicalName());
        m.put("FLOAT", double.class.getCanonicalName());
        m.put("VARBINARY", byte[].class.getCanonicalName());
        m.put("BINARY", byte[].class.getCanonicalName());
        m.put("LONGVARBINARY", byte[].class.getCanonicalName());
        m.put("DATE", Date.class.getCanonicalName());
        m.put("TIME", Time.class.getCanonicalName());
        m.put("TIMESTAMP", java.sql.Timestamp.class.getCanonicalName());
        m.put("CLOB", java.sql.Clob.class.getCanonicalName());
        m.put("BLOB", java.sql.Blob.class.getCanonicalName());
        m.put("ARRAY", Array.class.getCanonicalName());
        m.put("DISTINCT", Type.class.getCanonicalName());
        m.put("STRUCT", Struct.class.getCanonicalName());
        m.put("REF", Ref.class.getCanonicalName());
        m.put("DATALINK", java.net.URL.class.getCanonicalName());
        m.put("JAVA_OBJECT", Object.class.getCanonicalName());
        JDBC_JAVA_TYPES = Collections.unmodifiableMap(m);
    }
    /* package */ static boolean debugging = false;
    /* package */ static Logger log = Logger.getLogger("ezjdo");
    static {
    	String args = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().toString().toLowerCase();
        if (debugging != true && (args.indexOf("debug") > 0 || args.indexOf("jdwp") > 0)) {
            debugging = true;
            log.log(Level.INFO, "ezJDO Debugging enabled");
        }
    }
    
    public final static Pattern EMAIL_PATTERN = Pattern.compile("[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", Pattern.CASE_INSENSITIVE);
    
    public final static void setDebugLevel(Level level) {
        log.setLevel(level);
        //get the top Logger:
        Logger topLogger = Logger.getLogger("");
        // Handler for console (reuse it if it already exists)
        Handler consoleHandler = null;
        //see if there is already a console handler
        for (Handler handler : topLogger.getHandlers()) {
            if (handler instanceof ConsoleHandler) {
                //found the console handler
                consoleHandler = handler;
                break;
            }
        }
        if (consoleHandler == null) {
            //there was no console handler found, create a new one
            consoleHandler = new ConsoleHandler();
            topLogger.addHandler(consoleHandler);
        }
        //set the console handler to fine:
        if(level.intValue() < consoleHandler.getLevel().intValue()) {
        	consoleHandler.setLevel(level);
        }
        if (level.intValue() <= Level.OFF.intValue()) {
            log.log(Level.INFO, "ezJDO Debugging set to: {0}", level.getName());
            debugging = true;
        } else {
            debugging = false;
        }
    }
    private final static HashMap<Long, HashMap<String, Connection>> threadConnectionstringConnections = new HashMap<Long, HashMap<String, Connection>>();
    private static String[] dbName = new String[5];
    private static String[] tables = new String[5];
    private static String[] classes = new String[5];
    private static String[] sqlDriverClasses = new String[5];
    private static String[] connectionStrings = new String[5];
    private static Boolean[] saveKeys = new Boolean[5];
    private static String[][] attributes = new String[5][];
    private static String[][] attributeTypes = new String[5][];
    private static Integer[][] attributeLengths = new Integer[5][];
    private static Boolean[][] attributeNotNulls = new Boolean[5][];
    private static String[][] keys = new String[5][];
    private static Object[][] defaultValues = new Object[5][];
    private static Many[][][] mappings = new Many[5][][];
    private static MySQLQueryHelper mysqlQueryHelper;
    private static MSSQLQueryHelper mssqlQueryHelper;
    /* package */ static Integer[][] keyPos = new Integer[5][];
    /* package */ Object[] originalValues;
    /* package */ Field[] fields;
    /* package */ Boolean[] fieldsValid;
    /* package */ String[] fieldMessages;
    /* package */ Boolean newRecord = null;
    /* package */ int tableIndex = -1;

    /**
     * The Default constructor for BaseObject. The BaseObject constructors are
     * private, so they can only be called from inside of a extending class. The
     * constructor sets all default values and populates the cache for the
     * object. Calling this constructor marks the record as new, so if save() is
     * called the recored will be inserted into the database.
     *
     * @throws ezJDOException
     */
    protected BaseObject() {
        newRecord = true;
        try {
            init(getCaller().getClassName());
        } catch (ezJDOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * The extended constructor for BaseObject. The BaseObject constructors are
     * private, so they can only be called from inside of a extending class. The
     * constructor sets all default values and populates the cache for the
     * object. Calling this constructor marks the record as <b>not</b> new,
     * however this constructor does not execute a select statement. This
     * constructor would be used to update a record by its primary id(s) without
     * having to do a select statement first.
     *
     * @param keys the values of the primary keys that identify the object.
     * @throws ezJDOException
     */
    protected BaseObject(Object... keys) {
        try {
            newRecord = false;
            init(getCaller().getClassName());
            int k = 0;
            for (Object key : keys) {
                originalValues[keyPos[tableIndex][k]] = key;
                try {
                    fields[keyPos[tableIndex][k++]].set(this, key);
                } catch (IllegalArgumentException ex) {
                } catch (IllegalAccessException ex) {
                }
            }
            if (debugging) {
                log.log(Level.FINER, "BaseObject<{0}>({1})", new Object[]{toString(), join(keys, ",")});
            }
        } catch (ezJDOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Returns this Objects database keys.
     *
     * @return the objects keys.
     */
    public final String[] getKeys() throws ezJDOException {
        return keys[tableIndex];
    }

    /**
     * Returns this Objects database key values.
     *
     * @return the objects key values.
     */
    public final Object[] getKeyValues() {
        Object[] localKeys = new Object[keyPos[tableIndex].length];
        for (int i = 0; i < keyPos[tableIndex].length; i++) {
            localKeys[i] = originalValues[keyPos[tableIndex][i]];
        }
        return localKeys;
    }

    /**
     * Reloads all database values from the for the object. If the object is a
     * new object this methods just calls clear(). If the record for the object
     * was removed the object becomes a fresh new object.
     *
     * @throws ezJDOException
     * @see #clear()
     */
    public final void reload() throws ezJDOException {
        if (debugging) {
            log.log(Level.FINE, "BaseObject<{0}>.reload()", toString());
        }
        if (newRecord) {
            clear();
        } else {
            sqlFirst(this, "SELECT * FROM " + getTableName() + " WHERE " + getKeyWhereClause(), getKeyValues());
        }
    }

    /**
     * Overrides all of the fields for this object with those of the object
     * passed in, keys are not copied. The objects must be of the same class or
     * this method will throw an ezJDOException.
     *
     * @throws ezJDOException
     */
    public final <T extends BaseObject> void set(T obj) throws ezJDOException {
        if (debugging) {
            log.log(Level.FINE, "BaseObject<{0}>.set({1})", new Object[]{toString(), obj.toString()});
        }
        if (this.getClass().equals(obj.getClass())) {
            for (int i = 0; i < this.fields.length; i++) {
                try {
                    this.fields[i].set(this, obj.fields[i].get(obj));
                    this.originalValues[i] = obj.originalValues[i];
                } catch (IllegalArgumentException ex) {
                    log.log(Level.FINE, "You cannot call set() using object of different types.", ex);
                } catch (IllegalAccessException ex) {
                    log.log(Level.FINE, "You cannot call set() using object of different types.", ex);
                }
            }
        } else {
            throw new ezJDOException("You cannot call set() using object of different types.");
        }
    }

    /**
     * This function is used to mark fields as invalid inside of the save
     * function. If the boolean attribute test is false the field is considered
     * invalid.
     *
     * @throws ezJDOException
     */
    protected final Boolean validate(String attribute, Boolean test, String message) throws ezJDOException {

        int index = getIndexForAttribute(tableIndex, attribute);
        if (index == -1) {
            throw new ezJDOException("You cannot call validate on a attribute that does not exist. The attribute you wish to validate must match a column in the database.");
        } else {
            if (!test) {
                if (debugging) {
                    log.log(Level.FINEST, "BaseObject<{0}>.validate({1}}) => [false:\"{2}\"]", new Object[]{toString(), attribute, message});
                }
                fieldsValid[index] = false;
                fieldMessages[index] = message;
            } else {
                if (debugging) {
                    log.log(Level.FINEST, "BaseObject<{0}>.validate({1}) => [true]", new Object[]{toString(), attribute});
                }
                fieldsValid[index] = true;
            }
        }
        return test;
    }

    /**
     * Retrieves an array of field messages for the invalid fields.
     *
     * @return array of invalid field messages.
     * @throws ezJDOException
     */
    public final String[] getInvalidMessages() throws ezJDOException {
        ArrayList<String> messages = new ArrayList<String>();
        for (int i = 0; i < fieldsValid.length; i++) {
            if (!fieldsValid[i]) {
                messages.add(fieldMessages[i]);
            }
        }
        String[] amessages = new String[messages.size()];
        messages.toArray(amessages);
        return amessages;
    }

    /**
     * Retrieves an array of invalid fields.
     *
     * @return array of invalid fields.
     * @throws ezJDOException
     */
    public final Field[] getInvalidFields() throws ezJDOException {
        ArrayList<Field> localFields = new ArrayList<Field>();
        for (int i = 0; i < fieldsValid.length; i++) {
            if (!fieldsValid[i]) {
                localFields.add(this.fields[i]);
            }
        }
        Field[] afields = new Field[localFields.size()];
        localFields.toArray(afields);
        return afields;
    }

    /**
     * Resets all validation messages, and clears any field validation errors.
     *
     * @throws ezJDOException
     */
    public void clearValidation() throws ezJDOException {
        if (debugging) {
            log.log(Level.FINEST, "BaseObject<{0}>.clearValidation()", toString());
        }
        fieldsValid = new Boolean[attributes[tableIndex].length];
        fieldMessages = new String[attributes[tableIndex].length];
        for (int i = 0; i < attributes[tableIndex].length; i++) {
            fieldsValid[i] = true;
            fieldMessages[i] = null;
        }
    }

    /**
     * Saves the changes made to the object to the database.
     *
     * @return the number of rows affected.
     * @throws ezJDOException
     */
    public int save() throws ezJDOValidationException, ezJDOException {
        if (debugging) {
            log.log(Level.FINE, "BaseObject<{0}>.save()", toString());
        }
        int rowsAffected = 0;
        try {
            if (locallyModified()) {
                valid(); // check for validity
                if (newRecord) {
                    StringBuilder sb = new StringBuilder();
                    StringBuilder sb2 = new StringBuilder();
                    for (int i = 0; i < attributes[tableIndex].length; i++) {
                        if (saveKeys[tableIndex] || !isKeyAttributeAtPos(i)) { // not key attribute
                            sb.append(", ").append(attributes[tableIndex][i]).append("");
                            sb2.append(", ?");
                        }
                    }
                    String SQL = "INSERT INTO " + getTableName() + " (" + sb.substring(2) + ") VALUES (" + sb2.substring(2) + ")";
                    if (debugging) {
                        log.log(Level.INFO, SQL);
                    }
                    PreparedStatement prepStmt = getConnection(tableIndex, Thread.currentThread().getId()).prepareStatement(SQL, saveKeys[tableIndex] ? PreparedStatement.NO_GENERATED_KEYS : PreparedStatement.RETURN_GENERATED_KEYS);
                    int n = 1;
                    for (int i = 0; i < attributes[tableIndex].length; i++) {
                        if (saveKeys[tableIndex] || !isKeyAttributeAtPos(i)) { // not key attribute
                            try {
                                Object value = fields[i].get(this);
                                if (value instanceof byte[]) {
                                    byte[] b = (byte[]) (value);
                                    prepStmt.setBytes(n++, b);
                                } else {
                                    prepStmt.setObject(n++, value);
                                }
                                if (debugging) {
                                    log.log(Level.FINEST, "  * param: {0}", String.valueOf(value));
                                }
                            } catch (IllegalArgumentException ex) {
                                String className = getCaller().getClassName();
                                throw new ezJDOException("The call to save() failed. Please make sure the class \"" + className + "\" is implemented correctly.", ex);
                            } catch (IllegalAccessException ex) {
                                String className = getCaller().getClassName();
                                throw new ezJDOException("The call to save() failed. Please make sure the class \"" + className + "\" is implemented correctly.", ex);
                            }
                        }
                    }
                    rowsAffected = prepStmt.executeUpdate();
                    if (saveKeys[tableIndex]) {
                        for (int i = 0; i < keyPos[tableIndex].length; i++) {
                            try {
                                originalValues[keyPos[tableIndex][i]] = fields[keyPos[tableIndex][i]].get(this);
                            } catch (IllegalAccessException ex) {
                            } catch (IllegalArgumentException ex) {
                            }
                        }
                    } else {
                        ResultSet resultSet = prepStmt.getGeneratedKeys();
                        if (resultSet.next()) {
                            for (int i = 0; i < keyPos[tableIndex].length; i++) {
                                Object var = resultSet.getObject(i + 1);
                                if (var.getClass() == BigDecimal.class && fields[keyPos[tableIndex][i]].getType() == Integer.class) {
                                    originalValues[keyPos[tableIndex][i]] = ((BigDecimal) var).intValue();
                                } else {
                                    originalValues[keyPos[tableIndex][i]] = var;
                                }
                                try {
                                    fields[keyPos[tableIndex][i]].set(this, originalValues[keyPos[tableIndex][i]]);
                                } catch (IllegalAccessException ex) {
                                } catch (IllegalArgumentException ex) {
                                }
                                if (debugging) {
                                    log.log(Level.FINEST, " * genkey: {0}", String.valueOf(originalValues[keyPos[tableIndex][i]]));
                                }
                            }
                        }
                        resultSet.close();
                    }
                    prepStmt.close();
                } else {
                    Object[] values = new Object[attributes[tableIndex].length];
                    for (int i = 0; i < attributes[tableIndex].length; i++) {
                        try {
                            values[i] = fields[i].get(this);
                        } catch (IllegalAccessException ex) {
                        } catch (IllegalArgumentException ex) {
                        }
                    }
                    StringBuilder sb = new StringBuilder();
                    StringBuilder sb2 = new StringBuilder();
                    for (int i = 0; i < attributes[tableIndex].length; i++) {
                        if (!isKeyAttributeAtPos(i)) { // not key attribute
                            if (!(values[i] != null ? values[i].equals(originalValues[i]) : originalValues[i] != null ? originalValues[i].equals(values[i]) : true)) {
                                sb.append(", ").append(attributes[tableIndex][i]).append(" = ?");
                            }
                        } else {
                            sb2.append(", ").append(attributes[tableIndex][i]).append(" = ?");
                            if (saveKeys[tableIndex]) {
                                if (!(values[i] != null ? values[i].equals(originalValues[i]) : originalValues[i] != null ? originalValues[i].equals(values[i]) : true)) {
                                    sb.append(", ").append(attributes[tableIndex][i]).append(" = ?");
                                }
                            }
                        }
                    }
                    String SQL = "UPDATE " + getTableName() + " SET " + sb.substring(2) + " WHERE " + sb2.substring(2);
                    if (debugging) {
                        log.log(Level.INFO, SQL);
                    }
                    PreparedStatement prepStmt = getConnection(tableIndex, Thread.currentThread().getId()).prepareStatement(SQL);
                    int n = 1;
                    for (int i = 0; i < attributes[tableIndex].length; i++) {
                        if (saveKeys[tableIndex] || !isKeyAttributeAtPos(i)) { // not key attribute
                            if (!(values[i] != null ? values[i].equals(originalValues[i]) : originalValues[i] != null ? originalValues[i].equals(values[i]) : true)) {
                                if (values[i] instanceof byte[]) {
                                    byte[] b = (byte[]) (values[i]);
                                    prepStmt.setBytes(n++, b);
                                } else {
                                    prepStmt.setObject(n++, values[i]);
                                }
                                if (debugging) {
                                    log.log(Level.FINEST, "  * param: {0}", String.valueOf(values[i]));
                                }
                            }
                        }
                    }
                    for (int i = 0; i < keyPos[tableIndex].length; i++) {
                        prepStmt.setObject(n++, originalValues[keyPos[tableIndex][i]]);
                        if (debugging) {
                            log.log(Level.FINEST, "  * keyparam: {0}", String.valueOf(values[i]));
                        }
                    }
                    rowsAffected = prepStmt.executeUpdate();
                    prepStmt.close();
                }
            } else {
                if (debugging) {
                    log.log(Level.FINER, "BaseObject<{0}>.save() [nothing to save]", toString());
                }
            }
        } catch (SQLException ex) {
            String className = getCaller().getClassName();
            throw new ezJDOException("The call to save() failed. Please make sure the class \"" + className + "\" is implemented correctly.", ex);
        }
        newRecord = false;
        if (debugging) {
            log.log(Level.FINEST, "=> rowsAffected: {0}", rowsAffected);
        }
        return rowsAffected;
    }

    /**
     * delete the current object from the database.
     *
     * @return the number of rows affected.
     * @throws ezJDOException
     */
    public int delete() throws ezJDOValidationException, ezJDOException {
        if (debugging) {
            log.log(Level.FINE, "BaseObject<{0}>.delete()", toString());
        }
        int rowsAffected = 0;
        if (!newRecord) {
            valid(); // check for validity
            try {
                String SQL = "DELETE FROM " + getTableName() + " WHERE " + getKeyWhereClause();
                if (debugging) {
                    log.log(Level.INFO, SQL);
                }
                PreparedStatement prepStmt = getConnection(tableIndex, Thread.currentThread().getId()).prepareStatement(SQL);
                for (int i = 0; i < keyPos[tableIndex].length; i++) {
                    prepStmt.setObject(i + 1, originalValues[keyPos[tableIndex][i]]);
                    if (debugging) {
                        log.log(Level.FINEST, "  * param: {0}", String.valueOf(originalValues[keyPos[tableIndex][i]]));
                    }
                }
                rowsAffected = prepStmt.executeUpdate();
                prepStmt.close();
            } catch (SQLException ex) {
                String className = getCaller().getClassName();
                throw new ezJDOException("The call to delete() failed. Please make sure the class \"" + className + "\" implemented correctly.", ex);
            }
            clear();
        }
        if (debugging) {
            log.log(Level.FINEST, "=> rowsAffected: {0}", rowsAffected);
        }
        return rowsAffected;
    }

    /**
     * Retrieves locally modified the status of the attribute.
     *
     * @param attribute the attribute name to test.
     * @return <b>true</b> if the attribute has been changed from its original
     * value.<br/><b>false</b> if the value is the same as its original value.
     */
    public final Boolean locallyModified(String attribute) throws ezJDOException {
        int index = getIndexForAttribute(tableIndex, attribute);
        Object value = null;
        if (index < 0) {
            throw new ezJDOException("The attribute \"" + attribute + "\" is not a valid attribute for \"" + this.getClass().getName() + "\".");
        }
        try {
            value = fields[index].get(this);
        } catch (IllegalAccessException ex) {
        } catch (IllegalArgumentException ex) {
        }
        if (!(value != null ? value.equals(originalValues[index]) : originalValues[index] != null ? originalValues[index].equals(value) : true)) {
            return true;
        }
        return false;
    }

    /**
     * Retrieves locally modified the status of the object.
     *
     * @return <b>true</b> if any attributes have been changed from their
     * original values.<br/><b>false</b> if the values are the same as their
     * original values.
     */
    public final Boolean locallyModified() {
        for (int i = 0; i < originalValues.length; i++) {
            Object value = null;
            try {
                value = fields[i].get(this);
            } catch (IllegalAccessException ex) {
            } catch (IllegalArgumentException ex) {
            }
            if (!(value != null ? value.equals(originalValues[i]) : originalValues[i] != null ? originalValues[i].equals(value) : true)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Retrieves remotely modified the status of the object.
     *
     * @return <b>true</b> if attributes have been changed in the database since
     * the last time the object was retrieved.<br/><b>false</b> if the original
     * values are not changed in the database.
     * @throws ezJDOException
     */
    public final Boolean remotelyModified() throws ezJDOException {
        if (debugging) {
            log.log(Level.FINER, "BaseObject<{0}>.remotelyModified()", toString());
        }
        // TODO: use the compare operator
        if (!newRecord) {
            try {
                String SQL = "SELECT * FROM " + getTableName() + " WHERE " + getKeyWhereClause();
                if (debugging) {
                    log.log(Level.INFO, SQL);
                }
                PreparedStatement prepStmt = getConnection(tableIndex, Thread.currentThread().getId()).prepareStatement(SQL);
                prepStmt.setFetchSize(1);
                prepStmt.setMaxRows(1);
                for (int i = 0; i < keyPos[tableIndex].length; i++) {
                    prepStmt.setObject(i + 1, originalValues[keyPos[tableIndex][i]]);
                    if (debugging) {
                        log.log(Level.FINEST, "  * keyparam: {0}", String.valueOf(originalValues[keyPos[tableIndex][i]]));
                    }
                }
                ResultSet results = prepStmt.executeQuery();
                if (results.next()) {
                    for (int n = 1; n <= attributes[tableIndex].length; n++) {
                        int index = getIndexForAttribute(tableIndex, results.getMetaData().getColumnName(n));
                        if (index >= 0 && !((originalValues[index] != null) ? originalValues[index].equals(results.getObject(n)) : ((results.getObject(n) != null) ? results.getObject(n).equals(originalValues[index]) : true))) {
                            results.close();
                            prepStmt.close();
                            if (debugging) {
                                log.log(Level.FINER, "=> true", toString());
                            }
                            return true;
                        }
                    }
                }
                results.close();
                prepStmt.close();
                if (debugging) {
                    log.log(Level.FINER, "=> false", toString());
                }
                return false;
            } catch (SQLException ex) {
                String className = getCaller().getClassName();
                throw new ezJDOException("The call to remotelyModified() failed. Please make sure the class \"" + className + "\" implemented correctly.", ex);
            }
        }
        if (debugging) {
            log.log(Level.FINER, "=> false", toString());
        }
        return false;
    }

    /**
     * Identifies if the object is a new object or is from the database.
     *
     * @return <b>true</b> if the object is new.<br/><b>false</b> if the object
     * comes from the database.
     */
    public final Boolean isNew() {
        return newRecord;
    }

    /**
     * Restores all original values.
     *
     * @throws ezJDOException
     */
    public final void reset() throws ezJDOException {
        if (debugging) {
            log.log(Level.FINE, "BaseObject<{0}>.reset()", toString());
        }
        for (int i = 0; i < originalValues.length; i++) {
            try {
                fields[i].set(this, originalValues[i]);
            } catch (IllegalAccessException ex) {
            } catch (IllegalArgumentException ex) {
            }
        }
    }

    /**
     * Restores all default values and makes the object new again. but retains
     * any private or static variables
     *
     * @throws ezJDOException
     */
    public final void clear() throws ezJDOException {
        if (debugging) {
            log.log(Level.FINE, "BaseObject<{0}>.clear()", toString());
        }
        for (int i = 0; i < originalValues.length; i++) {
            originalValues[i] = defaultValues[tableIndex][i];
            try {
                fields[i].set(this, originalValues[i]);
            } catch (IllegalAccessException ex) {
            } catch (IllegalArgumentException ex) {
            }
        }
        newRecord = true;
    }

    /**
     * Identifies if the object the same and the data is equivalent to the other
     * object.
     *
     * @return <b>true</b> if the object is equal.<br/><b>false</b> if the
     * object is not equal.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof BaseObject)) {
            return false;
        }
        if (!Arrays.equals(getKeyValues(), ((BaseObject) obj).getKeyValues())) {
            return false;
        }
        for (int i = 0; i < fields.length; i++) {
            try {
                if (!fields[i].get(this).equals(((BaseObject) obj).fields[i].get(obj))
                        || !originalValues[i].equals(((BaseObject) obj).originalValues[i])) {
                    return false;
                }
            } catch (IllegalAccessException ex) {
                return false;
            } catch (IllegalArgumentException ex) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 17 * hash + Arrays.deepHashCode(this.originalValues);
        hash = 17 * hash + Arrays.deepHashCode(this.fields);
        return hash;
    }

    /**
     * Clones the object so the data is equivalent to the original object.
     *
     *
     * @return new cloned object.
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        try {
            Constructor c = this.getClass().getDeclaredConstructor();
            c.setAccessible(true);
            Object o = c.newInstance();
            ((BaseObject) o).set(this);
            ((BaseObject) o).newRecord = true;
            return o;
        } catch (ezJDOException ex) {
            throw new CloneNotSupportedException(this.getClass() + " could not be cloned please check your class declaration. " + ex.getLocalizedMessage());
        } catch (InstantiationException ex) {
            throw new CloneNotSupportedException(this.getClass() + " could not be cloned please check your class declaration. " + ex.getLocalizedMessage());
        } catch (IllegalAccessException ex) {
            throw new CloneNotSupportedException(this.getClass() + " could not be cloned please check your class declaration. " + ex.getLocalizedMessage());
        } catch (IllegalArgumentException ex) {
            throw new CloneNotSupportedException(this.getClass() + " could not be cloned please check your class declaration. " + ex.getLocalizedMessage());
        } catch (InvocationTargetException ex) {
            throw new CloneNotSupportedException(this.getClass() + " could not be cloned please check your class declaration. " + ex.getLocalizedMessage());
        } catch (NoSuchMethodException ex) {
            throw new CloneNotSupportedException(this.getClass() + " could not be cloned please check your class declaration. " + ex.getLocalizedMessage());
        } catch (SecurityException ex) {
            throw new CloneNotSupportedException(this.getClass() + " could not be cloned please check your class declaration. " + ex.getLocalizedMessage());
        }
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[" + ((this.isNew()) ? "new" : join(this.getKeyValues(), ",")) + "]";
    }

    /**
     * Overridable callback, invoked after the object is constructed from the
     * database.
     *
     */
    protected void postConstruct() throws ezJDOException {
    }

    /**
     * Returns objects in the database from defined mappings using a "many"
     * relationship.
     *
     * <p><i>This a wrapper for findManyWhere() passing in an empty
     * condition.</i>
     *
     * <p> A Many relationship is where the objects primary id(s) correspond to
     * "many" objects from another table where that table has a matching foreign
     * id.
     *
     * <p>This function is protected for internal use, so when it is exposed
     * publicly from inside of an <b>extending</b> class it can be wrapped in
     * order to simplify its use. For example:</p> <blockquote><pre>
     * public class DocType extends BaseObject {
     *
     *     &#064Many(object=Document.class,column="typeId") //   And Many Docuemnts
     *     private Integer id;                         // &lt;-- on primary id
     *
     *     // relational function
     *     public Results&lt;Document&gt; Documents() throws ezJDOException {
     *         return findMany(Document.class); // mapped above
     *     }
     * }
     * </pre></blockquote>
     *
     * @param <T> generic type that extends BaseObject.
     * @param clazz the class for the object you are searching for.
     * @return a collection of database objects. If the search does not find any
     * objects in the database the array list will be empty.
     * @throws ezJDOException
     * @see #findManyWhere(Class, String, Object...)
     */
    protected <T extends BaseObject> Results<T> findMany(Class<T> clazz) throws ezJDOException {
        if (debugging) {
            log.log(Level.FINER, "BaseObject<{0}>.findMany({1})", new Object[]{toString(), clazz});
        }
        return findManyWhere(clazz, "");
    }

    /**
     * Returns objects in the database from defined mappings using a "many"
     * relationship combined with a SQL where clause.
     *
     * <p> A Many relationship is where the objects primary id(s) correspond to
     * "many" objects from another table where that table has a matching foreign
     * id.
     *
     * <p>This function is protected for internal use, so when it is exposed
     * publicly from inside of an <b>extending</b> class it can be wrapped in
     * order to simplify its use. For example:</p> <blockquote><pre>
     * public class DocType extends BaseObject {
     *
     *     &#064Many(object=Document.class,column="typeId") //   And Many Docuemnts
     *     private Integer id;                         // &lt;-- on primary id
     *
     *     // relational function
     *     public Results&lt;Document&gt; oldDocuments() throws ezJDOException {
     *         return findManyWhere(Document.class,"created &lt; "+oldTime); // mapped above
     *     }
     * }
     * </pre></blockquote>
     *
     * @param <T> generic type that extends BaseObject.
     * @param clazz the class for the object you are searching for.
     * @param conditions the SQL where clause conditions string.
     * @return a collection of database objects. If the search does not find any
     * objects in the database the array list will be empty.
     * @throws ezJDOException
     */
    protected <T extends BaseObject> Results<T> findManyWhere(Class<T> clazz, String conditions, Object... values) throws ezJDOException {
        if (debugging && conditions != null && !conditions.trim().isEmpty()) {
            log.log(Level.FINER, "BaseObject<{0}>.findManyWhere({1})", new Object[]{toString(), clazz});
        }
        String className = this.getClass().getName();
        int baseTableIndex = getTableCacheIndex(clazz.getName());
        if (baseTableIndex < 0) {
            throw new ezJDOException("You did not pass the correct object class: \"" + clazz.getName() + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
        }
        String baseTableName = tables[baseTableIndex];
        Many mapping = null;
        int index = 0;
        found:
        for (int i = 0; i < keyPos[tableIndex].length; i++) {
            for (int n = 0; n < mappings[tableIndex][keyPos[tableIndex][i]].length; n++) {
                if (clazz == mappings[tableIndex][keyPos[tableIndex][i]][n].object()) {
                    mapping = mappings[tableIndex][keyPos[tableIndex][i]][n];
                    index = keyPos[tableIndex][i];
                    break found;
                }
            }
        }
        if (mapping == null) {
            throw new ezJDOException("You cannot call findManyWhere(" + clazz.getName() + ") from \"" + className + "\" because it has no mappings to \"" + className + "\" on its ids.");
        }
        if (conditions != null && !conditions.trim().isEmpty()) {
            Object[] newVals = new Object[values.length + 1];
            newVals[0] = originalValues[index];
            for (int i = 1; i < newVals.length; i++) {
                newVals[i] = values[i - 1];
            }
            values = newVals;
        } else {
            values = new Object[]{originalValues[index]};
        }
        String foreignColumn = mapping.column();
        if (foreignColumn.isEmpty()) {
            foreignColumn = className.substring(className.lastIndexOf(".") + 1).toLowerCase() + "Id";
        }
        String joinTable = mapping.joinTable();
        String joinString = "";
        if (joinTable.isEmpty()) {
            // if no join table is specified make sure that the base object has the foreignColumn
            int foreignIndex = getIndexForAttribute(baseTableIndex, foreignColumn);
            if (foreignIndex < 0) {
                // he base object does not have the foreignColumn, so assume a join table from the 2 objects tables
                if (baseTableName.compareTo(getTableName()) < 0) {
                    joinTable = baseTableName + getTableName();
                } else {
                    joinTable = getTableName() + baseTableName;
                }
            }
        }
        if (!joinTable.isEmpty()) {
            String joinColumn = mapping.joinColumn();
            if (joinColumn.isEmpty()) {
                String baseClassName = clazz.getName();
                joinColumn = baseClassName.substring(baseClassName.lastIndexOf(".") + 1).toLowerCase() + "Id";
            }
            joinString = " JOIN " + joinTable + " T2 ON T1." + attributes[baseTableIndex][index] + " = T2." + joinColumn + "";
        }

        String sql = "SELECT T1.* FROM " + baseTableName + " T1" + (joinString.isEmpty() ? "" : joinString) + " WHERE " + foreignColumn + " = ?" + (conditions == null || conditions.trim().isEmpty() ? "" : " AND (" + conditions + ")");
        return sql(clazz, sql, values);
    }

    /**
     * Finds a object in the database from its keys.
     *
     * <p>This function is protected for internal use, so when it is exposed
     * publicly from inside of an <b>extending</b> class it can be wrapped in
     * order to simplify its use. For example:</p> <blockquote><pre>
     * public class Document extends BaseObject {
     *
     *     // wrapper for find
     *     public static Document find(Integer id) throws ezJDOException {
     *         return find(Document.class, id);
     *     }
     *
     *     // relational function
     *     public DocType DocType() throws ezJDOException {
     *         return find(DocType.class, typeId);
     *         // typeId is an internal attribute for connecting the data relationship
     *     }
     * }
     * </pre></blockquote>
     *
     * @param <T> generic type that you want to return.
     * @param clazz the class for the object you want the results stored in.
     * @param keys the key values that identify the object in the database.
     * @return the database object found or a new object. If the search does not
     * find a object in the database it just returns a new object.
     * @throws ezJDOException
     */
    protected static <T> T find(Class<T> clazz, Object... keys) throws ezJDOException {
        if (debugging) {
            log.log(Level.FINER, "BaseObject.find({0},[{1}])", new Object[]{clazz, join(keys, ",")});
        }
        String className = clazz.getName();
        int baseTableIndex = getTableCacheIndex(clazz.getName());
        if (baseTableIndex < 0) {
            throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
        }
        String baseTableName = tables[baseTableIndex];
        String sql = "SELECT * FROM " + baseTableName + " WHERE " + getKeyWhereClause(baseTableIndex);
        return sqlFirst(clazz, sql, keys);
    }

    /**
     * Finds a object in the database from example.
     *
     * <p>This function is protected for internal use, so when it is exposed
     * publicly from inside of an <b>extending</b> class it can be wrapped in
     * order to simplify its use. For example:</p> <blockquote><pre>
     * public class Document extends BaseObject {
     *
     *     // wrapper for find
     *     public static Results&lt;Document&gt; find(Document example) throws ezJDOException {
     *         return find(Document.class, example);
     *     }
     * }
     * </pre></blockquote>
     *
     * @param <T> generic type that you want to return.
     * @param clazz the class for the object you want the results stored in.
     * @param example the example object you will use for searching.
     * @return a collection of database objects. If the search does not find any
     * objects in the database the array list will be empty.
     * @throws ezJDOException
     */
    protected static <T> Results<T> find(Class<T> clazz, T example) throws ezJDOException {
        if (debugging) {
            log.log(Level.FINER, "BaseObject.find({0},[{1}])", new Object[]{clazz, example});
        }
        String className = clazz.getName();
        int baseTableIndex = getTableCacheIndex(className);
        if (baseTableIndex < 0) {
            throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
        }
        StringBuilder sql = new StringBuilder();
        ArrayList<Object> values = new ArrayList<Object>();
        try {
            for (int i = 0; i < attributes[baseTableIndex].length; i++) {
                Object v = ((BaseObject) example).fields[i].get(example);
                if (v != null) {
                    if (values.size() > 0) {
                        sql.append(" AND ");
                    }
                    sql.append(attributes[baseTableIndex][i]).append(" = ?");
                    values.add(v);
                }
            }
        } catch (IllegalArgumentException ex) {
            throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
        } catch (IllegalAccessException ex) {
            throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
        }
        return findWhere(clazz, sql.toString(), values.toArray());
    }

    /**
     * Finds objects in the database from a SQL where clause.
     *
     * <p>This function is protected for internal use, so when it is exposed
     * publicly from inside of an <b>extending</b> class it can be wrapped in
     * order to simplify its use. For example:</p> <blockquote><pre>
     * public class Document extends BaseObject {
     *
     *     // wrapper for findWhere
     *     public static Document findWhere(String conditions) throws ezJDOException {
     *         return findWhere(Document.class, conditions);
     *     }
     *
     *     // relational function
     *     public ArrayList&lt;Contract&gt; Contracts() throws ezJDOException {
     *         return find(Contract.class, "contractNo = ?", contractNo);
     *         // contractNo is an internal attribute for connecting the data relationship
     *     }
     * }
     * </pre></blockquote>
     *
     * @param <T> generic type that you want to return.
     * @param clazz the class for the object you want the results stored in.
     * @param conditions the SQL where clause conditions string.
     * @return a collection of database objects. If the search does not find any
     * objects in the database the array list will be empty.
     * @throws ezJDOException
     */
    protected static <T> Results<T> findWhere(Class<T> clazz, String conditions, Object... values) throws ezJDOException {
        if (debugging) {
            log.log(Level.FINER, "BaseObject.findWhere({0})", clazz);
        }
        String className = clazz.getName();
        int baseTableIndex = getTableCacheIndex(clazz.getName());
        if (baseTableIndex < 0) {
            throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
        }
        String baseTableName = tables[baseTableIndex];
        String sql = "SELECT * FROM " + baseTableName + "" + (conditions == null || conditions.trim().isEmpty() ? "" : " WHERE " + conditions);
        return sql(clazz, sql, values);
    }

    /**
     * Finds objects in the database.
     *
     * <p>This function is protected for internal use, so when it is exposed
     * publicly from inside of an <b>extending</b> class it can be wrapped in
     * order to simplify its use. For example:</p> <blockquote><pre>
     * public class Document extends BaseObject {
     *
     *     // wrapper for findAll
     *     public static Document findAll() throws ezJDOException {
     *         return findAll(Document.class);
     *     }
     * }
     * </pre></blockquote>
     *
     * @param <T> generic type that you want to return.
     * @param clazz the class for the object you want the results stored in.
     * @return a collection of database objects. If the search does not find any
     * objects in the database the array list will be empty.
     * @throws ezJDOException
     */
    protected static <T> Results<T> findAll(Class<T> clazz) throws ezJDOException {
        if (debugging) {
            log.log(Level.FINER, "BaseObject.findAll({0})", clazz);
        }
        String className = clazz.getName();
        int baseTableIndex = getTableCacheIndex(clazz.getName());
        if (baseTableIndex < 0) {
            throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
        }
        String baseTableName = tables[baseTableIndex];
        String sql = "SELECT * FROM " + baseTableName;
        return sql(clazz, sql);
    }

    /**
     * Finds the first object in the database from a SQL where clause.
     *
     * <p>This function is protected for internal use, so when it is exposed
     * publicly from inside of an <b>extending</b> class it can be wrapped in
     * order to simplify its use. For example:</p> <blockquote><pre>
     * public class Document extends BaseObject {
     *
     *     // wrapper for findFirst
     *     public static Document findFirst(String conditions) throws ezJDOException {
     *         return findFirst(Document.class, conditions);
     *     }
     *
     *     // relational function
     *     public Contract firstContract() throws ezJDOException {
     *         return findFirst(Contract.class, "contractNo = ?", contractNo);
     *         // contractNo is an internal attribute for connecting the data relationship
     *     }
     * }
     * </pre></blockquote>
     *
     * @param <T> generic type that you want to return.
     * @param clazz the class for the object you want the results stored in.
     * @param conditions the SQL where clause conditions string.
     * @return the database object found or a new object. If the search does not
     * find a object in the database it just returns a new object.
     * @throws ezJDOException
     */
    protected static <T> T findFirst(Class<T> clazz, String conditions, Object... values) throws ezJDOException {
        if (debugging) {
            log.log(Level.FINER, "BaseObject.findFirst({0})", clazz);
        }
        String className = clazz.getName();
        int baseTableIndex = getTableCacheIndex(clazz.getName());
        if (baseTableIndex < 0) {
            throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
        }
        String baseTableName = tables[baseTableIndex];
        String sql = "SELECT * FROM " + baseTableName + "" + (conditions == null || conditions.trim().isEmpty() ? "" : " WHERE " + conditions);
        return sqlFirst(clazz, sql, values);
    }

    /**
     * Finds the last object in the database from a SQL where clause.
     *
     * <p>This function is protected for internal use, so when it is exposed
     * publicly from inside of an <b>extending</b> class it can be wrapped in
     * order to simplify its use. For example:</p> <blockquote><pre>
     * public class Document extends BaseObject {
     *
     *     // wrapper for findLast
     *     public static Document findLast(String conditions) throws ezJDOException {
     *         return findLast(Document.class, conditions);
     *     }
     *
     *     // relational function
     *     public Contract lastContract() throws ezJDOException {
     *         return findLast(Contract.class, "contractNo = ?", contractNo);
     *         // contractNo is an internal attribute for connecting the data relationship
     *     }
     * }
     * </pre></blockquote>
     *
     * @param <T> generic type that you want to return.
     * @param clazz the class for the object you want the results stored in.
     * @param conditions the SQL where clause conditions string.
     * @return the database object found or a new object. If the search does not
     * find a object in the database it just returns a new object.
     * @throws ezJDOException
     */
    protected static <T> T findLast(Class<T> clazz, String conditions, Object... values) throws ezJDOException {
        if (debugging) {
            log.log(Level.FINER, "BaseObject.findLast({0})", clazz);
        }
        String className = clazz.getName();
        int baseTableIndex = getTableCacheIndex(className);
        if (baseTableIndex < 0) {
            throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
        }
        StringBuilder keysWhere = new StringBuilder();
        for (int i = 0; i < keyPos[baseTableIndex].length; i++) {
            if (i > 0) {
                keysWhere.append(", ");
            }
            keysWhere.append(BaseObject.keys[baseTableIndex][i]).append(" DESC");
        }
        String baseTableName = tables[baseTableIndex];
        String sql = "SELECT * FROM " + baseTableName + "" + (conditions == null || conditions.trim().isEmpty() ? "" : " WHERE " + conditions) + " ORDER BY " + keysWhere.toString();
        return sqlFirst(clazz, sql, values);
    }

    /**
     * Warning this is an advanced function that should be used carefully. Most
     * of the time you should be able to use your find methods to get the data
     * you need.
     *
     * <p>this functions will issue a select statement and try to map the values
     * back to an ArrayList of objects from the type passed.</p>
     *
     * @param <T> generic type that you want to return.
     * @param clazz the class for the object you want the results stored in.
     * @param sql the SQL statement string.
     * @param values an array of values that replace the ? in the SQL statement.
     * @return a collection of database objects. If the search does not find any
     * objects in the database the array list will be empty.
     * @throws ezJDOException
     */
    protected static <T> Results<T> sql(Class<T> clazz, String sql, Object... values) throws ezJDOException {
        if (debugging) {
            log.log(Level.FINEST, "BaseObject.sql({0})", clazz);
        }
        Results<T> found;
        try {
            String className;
            if (clazz != null && BaseObject.class.isAssignableFrom(clazz)) {
                className = clazz.getName();
            } else {
                className = getCaller().getClassName();
            }
            int baseTableIndex = getTableCacheIndex(className);
            if (baseTableIndex < 0) {
                throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
            }
            String tmpSQL = (" " + sql).toLowerCase();
            boolean update = tmpSQL.contains(" update ") || tmpSQL.contains(";update ");
            if (clazz != null && update) {
                throw new ezJDOException("You cannot call update statments and pass in a return class, there is no way to get databack from a update beside updateCount. You must use null for the class when doing a update.");
            }
            boolean insert = tmpSQL.contains(" insert ") || tmpSQL.contains(";insert ");
            PreparedStatement prepStmt = getConnection(baseTableIndex, Thread.currentThread().getId()).prepareStatement(sql, ((clazz != null && insert) ? PreparedStatement.RETURN_GENERATED_KEYS : PreparedStatement.NO_GENERATED_KEYS));
            int i = 1;
            for (Object value : values) {
                if (debugging) {
                    log.log(Level.FINEST, "  * param: {0}", String.valueOf(value));
                }
                prepStmt.setObject(i++, value);
            }
            found = new Results<T>(clazz, prepStmt, sql, values);
        } catch (SecurityException ex) {
            throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
        } catch (IllegalArgumentException ex) {
            throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
        } catch (SQLException ex) {
            throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
        }
        return found;
    }

    /**
     * Warning this is an advanced function that should be used carefully. Most
     * of the time you should be able to use your find methods to get the data
     * you need.
     *
     * <p>this functions will issue a sql statement and try to map the values
     * back to a new instance of the passed object class</p>
     *
     * @param <T> generic type that you want to return.
     * @param clazz the class for the object you want the results stored in.
     * @param sql the SQL statement string.
     * @param values an array of values that replace the ? in the SQL statement.
     * @return the database object found or a new object. If the search does not
     * find a object in the database it just returns a new object.
     * @throws ezJDOException
     */
    protected static <T> T sqlFirst(Class<T> clazz, String sql, Object... values) throws ezJDOException {
        Object found;
        try {
            if (BaseObject.class.isAssignableFrom(clazz)) {
                Constructor c = clazz.getDeclaredConstructor();
                c.setAccessible(true);
                found = c.newInstance();
                sqlFirst(found, sql, values);
            } else {
                if (debugging) {
                    log.log(Level.FINEST, "BaseObject.selectFirst({0})", clazz);
                }
                Object[] o;
                try {
                    String className = getCaller().getClassName();
                    int baseTableIndex = getTableCacheIndex(className);
                    if (baseTableIndex < 0) {
                        throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
                    }
                    if (BaseObject.debugging) {
                        log.log(Level.INFO, sql);
                    }
                    PreparedStatement prepStmt = getConnection(baseTableIndex, Thread.currentThread().getId()).prepareStatement(sql);
                    int n = 1;
                    for (Object value : values) {
                        prepStmt.setObject(n++, value);
                        if (debugging) {
                            log.log(Level.FINEST, "  * param: {0}", String.valueOf(value));
                        }
                    }
                    prepStmt.setFetchSize(1);
                    prepStmt.setMaxRows(1);
                    //long start = System.currentTimeMillis();
                    ResultSet results = prepStmt.executeQuery();
                    //System.out.println(System.currentTimeMillis() - start);
                    o = new Object[results.getMetaData().getColumnCount()];
                    if (results.next()) {
                        construct(o, results);
                    }
                    results.close();
                    prepStmt.close();
                } catch (SQLException ex) {
                    throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
                }
                if (clazz.isArray()) {
                    found = o;
                } else {
                    found = o[0];
                }
                if (debugging) {
                    log.log(Level.FINEST, "=> {0}", found);
                }
            }
        } catch (InstantiationException ex) {
            throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class");
        } catch (IllegalAccessException ex) {
            throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class");
        } catch (IllegalArgumentException ex) {
            throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class");
        } catch (InvocationTargetException ex) {
            throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class");
        } catch (NoSuchMethodException ex) {
            throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class");
        } catch (SecurityException ex) {
            throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class");
        }
        try {
            return clazz.cast(found);
        } catch (ClassCastException ex) {
            throw new ezJDOException("You cannot cast the value returned by the statment to an " + clazz + ". See next exception.", ex);
        }
    }

    /**
     * Warning this is an advanced function that should be used carefully. Most
     * of the time you should be able to use your find methods to get the data
     * you need.
     *
     * <p>this functions will issue a stored procedure exec and will try to map
     * the values back to a Results of the passed class</p>
     *
     * @param <T> generic type that you want to return.
     * @param clazz the class for the object you want the results stored in.
     * @param sql the SQL statement string.
     * @param values an array of values that replace the ? in the SQL statement.
     * @throws ezJDOException
     */
    protected static <T> Results<T> exec(Class<T> clazz, String sql, Object... values) throws ezJDOException {
        if (debugging) {
            log.log(Level.FINEST, "BaseObject.exec({0})", clazz);
        }
        Results<T> found = null;
        try {
            String className;
            if (clazz != null && BaseObject.class.isAssignableFrom(clazz)) {
                className = clazz.getName();
            } else {
                className = getCaller().getClassName();
            }
            int baseTableIndex = getTableCacheIndex(className);
            if (baseTableIndex < 0) {
                throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
            }
            CallableStatement callStmt = getConnection(baseTableIndex, Thread.currentThread().getId()).prepareCall("{call " + sql + "}");
            int i = 1;
            for (Object value : values) {
                callStmt.setObject(i++, value);
                if (debugging) {
                    log.log(Level.FINEST, "  * param: {0}", String.valueOf(value));
                }
            }
            found = new Results<T>(clazz, callStmt);
        } catch (SecurityException ex) {
            throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
        } catch (IllegalArgumentException ex) {
            throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
        } catch (SQLException ex) {
            throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
        }
        return found;
    }

    /**
     * Deletes all records in the table for a specific object. Warning this will
     * remove all records, and should be used with caution.
     *
     * <p>This function is protected for internal use, so when it is exposed
     * publicly from inside of an <b>extending</b> class it can be wrapped in
     * order to simplify its use. For example:</p> <blockquote><pre>
     * public class Document extends BaseObject {
     *
     *     // wrapper for deleteAll
     *     public static Document deleteAll() throws ezJDOException {
     *         return deleteAll(Document.class);
     *     }
     * }
     * </pre></blockquote>
     *
     * @param <T> generic type that extends BaseObject
     * @param clazz the class for the object you are searching for.
     * @return the number of records deleted.
     * @throws ezJDOException
     */
    protected static <T extends BaseObject> int deleteAll(Class<T> clazz, String conditions, Object... values) throws ezJDOException {
        if (debugging) {
            log.log(Level.FINER, "BaseObject.deleteAll({0})", clazz);
        }
        int rowsAffected = 0;
        String className = clazz.getName();
        try {
            int baseTableIndex = getTableCacheIndex(className);
            if (baseTableIndex < 0) {
                throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
            }
            String sql = "DELETE FROM " + tables[baseTableIndex] + "" + (conditions == null || conditions.trim().isEmpty() ? "" : " WHERE " + conditions);
            if (BaseObject.debugging) {
                log.log(Level.INFO, sql);
            }
            PreparedStatement prepStmt = getConnection(baseTableIndex, Thread.currentThread().getId()).prepareStatement(sql);
            int i = 1;
            for (Object value : values) {
                if (debugging) {
                    log.log(Level.FINEST, "  * param: {0}", String.valueOf(value));
                }
                prepStmt.setObject(i++, value);
            }
            rowsAffected = prepStmt.executeUpdate();
            prepStmt.close();
        } catch (SQLException ex) {
            throw new ezJDOException("The call to deleteAll() failed. Please make sure the class \"" + className + "\" implemented correctly.", ex);
        }
        if (debugging) {
            log.log(Level.FINEST, "=> rowsAffected: {0}", rowsAffected);
        }
        return rowsAffected;
    }

    /* package */ static void sqlFirst(Object base, String sql, Object... values) throws ezJDOException {
        if (debugging) {
            log.log(Level.FINEST, "BaseObject.sqlFirst({0})", base);
        }
        try {
            String className;
            if (BaseObject.class.isAssignableFrom(base.getClass())) {
                className = base.getClass().getName();
            } else if (base.getClass().isArray()) {
                className = getCaller().getClassName();
            } else {
                throw new ezJDOException("You cannot pass by reference in java, try using the class wraper as the parameter.");
            }
            int baseTableIndex = getTableCacheIndex(className);
            if (baseTableIndex < 0) {
                throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
            }
            if (BaseObject.debugging) {
                log.log(Level.INFO, sql);
            }
            PreparedStatement prepStmt = getConnection(baseTableIndex, Thread.currentThread().getId()).prepareStatement(sql);
            int i = 1;
            for (Object value : values) {
                prepStmt.setObject(i++, value);
                if (debugging) {
                    log.log(Level.FINEST, "  * param: {0}", String.valueOf(value));
                }
            }
            prepStmt.setFetchSize(1);
            prepStmt.setMaxRows(1);
            //long start = System.currentTimeMillis();
            ResultSet results = prepStmt.executeQuery();
            //System.out.println(System.currentTimeMillis() - start);
            if (results.next()) {
                construct(base, results);
            }
            results.close();
            prepStmt.close();
            if (debugging) {
                log.log(Level.FINEST, "=> {0}", base);
            }
        } catch (SQLException ex) {
            throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
        }
    }

    /*package*/ String getTableName() {
        return tables[tableIndex];
    }

    /*package*/ String getClassName() {
        return classes[tableIndex];
    }

    /*package*/ static String getClassTableName(String className) throws ezJDOException {
        int localTableIndex = getTableCacheIndex(className);
        if (localTableIndex == -1) {
            return "";
        }
        return tables[localTableIndex];
    }

    /*package*/ static String getTableClassName(String tableName) {
        int localTableIndex = -1;
        for (int i = 0; i < tables.length; i++) {
            if (tableName.equals(tables[i])) {
                localTableIndex = i;
                break;
            }
        }
        if (localTableIndex == -1) {
            return null;
        }
        return classes[localTableIndex];
    }

    /* package */ static String[] getTableKeys(String tableName) {
        int localTableIndex = -1;
        for (int i = 0; i < tables.length; i++) {
            if (tableName.equals(tables[i])) {
                localTableIndex = i;
                break;
            }
        }
        if (localTableIndex == -1) {
            return new String[0];
        }
        return keys[localTableIndex];
    }

    /* package */ static String getTableAttributeSQLType(String tableName, String attribute) throws ezJDOException {
        String className = getTableClassName(tableName);
        if (className == null) {
            return null;
        }
        int localTableIndex = getTableCacheIndex(className);
        if (localTableIndex == -1) {
            return null;
        }
        int localTableAttributeIndex = getIndexForAttribute(localTableIndex, attribute);
        if (localTableAttributeIndex == -1) {
            return null;
        }
        return attributeTypes[localTableIndex][localTableAttributeIndex];
    }

    /* package */ static SQLQueryHelper getSQLQueryHelper(String className) throws ezJDOException {
        int index = getTableCacheIndex(className);
        if (index != -1) {
            if (dbName[index].toLowerCase().contains("mysql")) {
                if (mysqlQueryHelper == null) {
                    mysqlQueryHelper = new MySQLQueryHelper();
                }
                return mysqlQueryHelper;
            } else if (dbName[index].toLowerCase().contains("microsoft")) {
                if (mssqlQueryHelper == null) {
                    mssqlQueryHelper = new MSSQLQueryHelper();
                }
                return mssqlQueryHelper;
            }
        }
        throw new ezJDOException("Could not get the SQLQueryHelper (" + dbName[index] + ") for the BaseObject class");
    }

    /* package */ static <T> T construct(Class<T> clazz, ResultSet resultset) throws ezJDOException {
        T base = null;
        if (BaseObject.class.isAssignableFrom(clazz)) {
            String className = clazz.getName();
            int baseTableIndex = getTableCacheIndex(className);
            if (baseTableIndex < 0) {
                throw new ezJDOException("You did not pass the correct object class: \"" + className + "\". You must specify a class that extends \"ezjdo.com.BaseObject\".");
            }
            try {
                Constructor c = clazz.getDeclaredConstructor();
                c.setAccessible(true); // solution
                base = clazz.cast(c.newInstance());
                construct((BaseObject) base, resultset);
            } catch (NoSuchMethodException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (SecurityException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (InstantiationException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (IllegalAccessException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (IllegalArgumentException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (InvocationTargetException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            }
        } else if (clazz.isArray()) {
            Object abase;
            try {
                int size = resultset.getMetaData().getColumnCount();
                abase = Array.newInstance(clazz.getComponentType(), size);
                for (int i = 0; i < size; i++) {
                    ((Object[]) abase)[i] = resultset.getObject(i + 1);
                }
                base = clazz.cast(abase);
                if (debugging) {
                    log.log(Level.FINEST, "construct({0}) => {1}", new Object[]{clazz.getName(), String.valueOf(base)});
                }
            } catch (SQLException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            }
        } else if (!Number.class.isAssignableFrom(clazz) && !String.class.isAssignableFrom(clazz)) {
            try {
                Constructor c;
                c = clazz.getDeclaredConstructor();
                c.setAccessible(true); // solution
                base = clazz.cast(c.newInstance());
                int size = resultset.getMetaData().getColumnCount();
                for (int i = 0; i < size; i++) {
                    String columnName = resultset.getMetaData().getColumnName(i + 1);
                    Field field = clazz.getDeclaredField(columnName);
                    field.setAccessible(true);
                    field.set(base, resultset.getObject(i + 1));
                }
            } catch (InstantiationException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (IllegalAccessException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (IllegalArgumentException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (InvocationTargetException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (NoSuchMethodException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (NoSuchFieldException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (SecurityException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (SQLException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            }
        } else {
            try {
                base = clazz.cast(resultset.getObject(1));
                if (debugging) {
                    log.log(Level.FINEST, "construct({0}) => {1}", new Object[]{clazz.getName(), String.valueOf(base)});
                }
            } catch (SQLException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            }
        }
        return base;
    }

    /* package */ static void construct(Object o, ResultSet resultset) throws ezJDOException {
        if (BaseObject.class.isAssignableFrom(o.getClass())) {
            try {
                BaseObject base = (BaseObject) o;
                base.newRecord = false;
                int size = attributes[base.tableIndex].length;
                base.originalValues = new Object[size];
                for (int n = 0; n < size; n++) {
                    base.originalValues[n] = resultset.getObject(attributes[base.tableIndex][n]);
                    try {
                        base.fields[n].set(base, base.originalValues[n]);
                    } catch (IllegalArgumentException ex) {
                        throw new ezJDOException("The type defined for the " + attributes[base.tableIndex][n] + " attribute does not match the data comming from the database.\n" + ex.getLocalizedMessage() + ". Please choose the correct type for the attribute. ");
                    } catch (NullPointerException ex) {
                        throw new ezJDOException("The " + o.getClass().getName() + " class is missing the attribute " + attributes[base.tableIndex][n] + ". You must create all attributes from the table it is referencing.", ex);
                    }
                }
                base.postConstruct();
                if (debugging) {
                    log.log(Level.FINER, "BaseObject<{0}>()", base);
                }
            } catch (SecurityException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (IllegalAccessException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            } catch (SQLException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            }
        } else if (o.getClass().isArray()) {
            try {
                int size1 = ((Object[]) o).length;
                int size2 = resultset.getMetaData().getColumnCount();
                for (int i = 0; i * 2 < size1 + size2; i++) {
                    ((Object[]) o)[i] = resultset.getObject(i + 1);
                }
                if (debugging) {
                    log.log(Level.FINEST, "construct() => {0}", String.valueOf(o));
                }
            } catch (SQLException ex) {
                throw new ezJDOException("The conditions for the method are not in correct SQL syntax, or you are not passing the correct BaseObject class", ex);
            }
        }
    }

    private void valid() throws ezJDOValidationException {
        // check nulls

        StringBuilder exceptionMessage = new StringBuilder("Vaidation failed on save(), Reasons:");
        Boolean errors = false;
        for (Integer i = 0; i < fieldsValid.length; i++) {
            try {
                Object o = fields[i].get(this);
                if (attributeNotNulls[tableIndex][i] && o == null) {
                    fieldsValid[i] = false;
                    fieldMessages[i] = attributes[tableIndex][i] + " cannot be null";
                }
                if (o != null && o instanceof String && ((String) o).length() > attributeLengths[tableIndex][i]) {
                    fieldsValid[i] = false;
                    fieldMessages[i] = attributes[tableIndex][i] + " cannot be longer than " + attributeLengths[tableIndex][i];
                }
            } catch (IllegalArgumentException ex) {
                fieldsValid[i] = false;
                fieldMessages[i] = ex.getLocalizedMessage();
            } catch (IllegalAccessException ex) {
                fieldsValid[i] = false;
                fieldMessages[i] = ex.getLocalizedMessage();
            }
            if (!fieldsValid[i]) {
                errors = true;
                exceptionMessage.append("\n * ").append(fieldMessages[i]);
            }
        }
        if (errors) {
            throw new ezJDOValidationException(exceptionMessage.toString());
        }
    }

    private Boolean isKeyAttributeAtPos(int pos) {
        for (int i = 0; i < keyPos[tableIndex].length; i++) {
            if (keyPos[tableIndex][i] == pos) {
                return true;
            }
        }
        return false;
    }

    private static int getIndexForAttribute(int tableIndex, String attribute) throws ezJDOException {
        int i = 0;
        for (String name : attributes[tableIndex]) {
            if (name.equals(attribute)) {
                return i;
            }
            i++;
        }
        return -1;
    }

    private static String getKeyWhereClause(Integer tableIndex) throws ezJDOException {
        StringBuilder keysWhere = new StringBuilder();
        for (int i = 0; i < keyPos[tableIndex].length; i++) {
            if (i > 0) {
                keysWhere.append(" AND ");
            }
            keysWhere.append("").append(BaseObject.keys[tableIndex][i]).append(" = ?");
        }
        return keysWhere.toString();
    }

    private String getKeyWhereClause() throws ezJDOException {
        StringBuilder keysWhere = new StringBuilder();
        for (int i = 0; i < keyPos[tableIndex].length; i++) {
            if (i > 0) {
                keysWhere.append(" AND ");
            }
            keysWhere.append("").append(BaseObject.keys[tableIndex][i]).append(" = ?");
        }
        return keysWhere.toString();
    }

    private String getSQLTypeFromJava(String javaType) throws ezJDOException {
        for (Entry<String, String> entry : JDBC_JAVA_TYPES.entrySet()) {
            if (javaType.equals(entry.getValue())) {
                Field[] sqlfields = java.sql.Types.class.getFields();
                try {
                    ResultSet resultSet = getConnection(tableIndex, Thread.currentThread().getId()).getMetaData().getTypeInfo();
                    while (resultSet.next()) {
                        if (resultSet.getString("TYPE_NAME").equals(entry.getKey())) {
                            for (int i = 0; i < sqlfields.length; i++) {
                                if (sqlfields[i].getName().equals(entry.getKey())) {
                                    if ((Integer) sqlfields[i].get(null) == resultSet.getInt("DATA_TYPE")) {
                                        resultSet.close();
                                        return entry.getKey();
                                    }
                                }
                            }
                        }
                    }
                    resultSet.close();
                } catch (IllegalAccessException e) {
                } catch (SQLException ex) {
                }
            }
        }

        return null;
    }

    private Integer getSQLTypeLengthFromJava(String javaType) throws ezJDOException {
        for (Entry<String, String> entry : JDBC_JAVA_TYPES.entrySet()) {
            if (javaType.equals(entry.getValue())) {
                Field[] sqlfields = java.sql.Types.class.getFields();
                try {
                    ResultSet resultSet = getConnection(tableIndex, Thread.currentThread().getId()).getMetaData().getTypeInfo();
                    while (resultSet.next()) {
                        if (resultSet.getString("TYPE_NAME").equals(entry.getKey())) {
                            for (int i = 0; i < sqlfields.length; i++) {
                                if (sqlfields[i].getName().equals(entry.getKey())) {
                                    if ((Integer) sqlfields[i].get(null) == resultSet.getInt("DATA_TYPE")) {
                                        Integer pre = resultSet.getInt("PRECISION");
                                        resultSet.close();
                                        return pre;
                                    }
                                }
                            }
                        }
                    }
                    resultSet.close();
                } catch (IllegalAccessException e) {
                } catch (SQLException ex) {
                }
            }
        }
        return null;
    }

    private Class<?> getPrimitiveType(Class<?> clazz) {
        if (clazz.isAssignableFrom(Boolean.class)) {
            return boolean.class;
        } else if (clazz.isAssignableFrom(Byte.class)) {
            return byte.class;
        } else if (clazz.isAssignableFrom(Character.class)) {
            return char.class;
        } else if (clazz.isAssignableFrom(Double.class)) {
            return double.class;
        } else if (clazz.isAssignableFrom(Float.class)) {
            return float.class;
        } else if (clazz.isAssignableFrom(Integer.class)) {
            return int.class;
        } else if (clazz.isAssignableFrom(Long.class)) {
            return long.class;
        } else if (clazz.isAssignableFrom(Short.class)) {
            return short.class;
        }
        return clazz;
    }

    private boolean isIntegerSQLType(String sqlType) throws ezJDOException {

        try {
            ResultSet resultSet = getConnection(tableIndex, Thread.currentThread().getId()).getMetaData().getTypeInfo();
            while (resultSet.next()) {
                if (resultSet.getString("TYPE_NAME").equals(sqlType)) {

                    Integer type = resultSet.getInt("DATA_TYPE");
                    if (type == java.sql.Types.BIGINT
                            || type == java.sql.Types.DECIMAL
                            || type == java.sql.Types.INTEGER
                            || type == java.sql.Types.NUMERIC
                            || type == java.sql.Types.SMALLINT
                            || type == java.sql.Types.TINYINT) {
                        resultSet.close();
                        return true;
                    }
                }
            }
            resultSet.close();
        } catch (SQLException ex) {
        }
        return false;
    }

    private void alterAddPrimaryKey(String columnName, String sqlType, Integer length, boolean autoinc) throws SQLException, ezJDOException {
        StringBuilder alterStmt = new StringBuilder();
        if ("mysql".equalsIgnoreCase(dbName[tableIndex])) {
            alterStmt.append("alter table ").append(getTableName())
                    .append(" add primary key (").append(columnName).append(")");
            if (BaseObject.debugging) {
                log.log(Level.INFO, alterStmt.toString());
            }
            getConnection(tableIndex, Thread.currentThread().getId()).prepareStatement(alterStmt.toString()).executeUpdate();
            alterStmt = new StringBuilder();
            alterStmt.append("alter table ").append(getTableName())
                    .append(" modify column ").append(columnName)
                    .append(" ").append(sqlType)
                    .append("(").append(length).append(")")
                    .append(autoinc ? " auto_increment" : "");
            if (BaseObject.debugging) {
                log.log(Level.INFO, alterStmt.toString());
            }
            getConnection(tableIndex, Thread.currentThread().getId()).prepareStatement(alterStmt.toString()).executeUpdate();
        }
        //mssql
        //ALTER TABLE dbo.YourTable ADD ID INT IDENTITY
        //ALTER TABLE dbo.YourTable ADD CONSTRAINT PK_YourTable PRIMARY KEY(ID)
        //oracle
        //create sequence t1_seq start with 1 increment by 1 nomaxvalue;
        //create trigger t1_trigger before insert on t1 for each row begin select t1_seq.nextval into :new.id from dual; end;
        //postgre
        //CREATE SEQUENCE mytable_myid_seq;
        //ALTER TABLE mytable ADD myid INT UNIQUE;
        //ALTER TABLE mytable ALTER COLUMN myid SET DEFAULT NEXTVAL('mytable_myid_seq');
        //UPDATE mytable SET myid = NEXTVAL('mytable_myid_seq');
    }

    private void init(String className) throws ezJDOException {
        synchronized (tables) {
            // detect cache for table or set it
            tableIndex = getTableCacheIndex(className);
            if (tableIndex == -1) {
                log.log(Level.FINEST, "Populating table cache for {0}", className);
                StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
                if (stackTrace.length > 10 && "init".equals(stackTrace[10].getMethodName()) && "ezjdo.com.BaseObject".equals(stackTrace[10].getClassName())) {
                    return;
                }

                Class clazz = null;
                String tableName = null, sqlDriverClass = null, connectionString = null;
                Boolean localSaveKeys = false;
                try {
                    clazz = Class.forName(className);
                } catch (ClassNotFoundException ex) {
                }
                // get class connection and table name info
                if (clazz != null) {
                    Annotation[] annotations = clazz.getAnnotations();
                    for (Annotation annotation : annotations) {
                        if (annotation instanceof BaseConfig) {
                            BaseConfig connAnnotation = (BaseConfig) annotation;
                            tableName = connAnnotation.tableName();
                            sqlDriverClass = connAnnotation.sqlDriverClass();
                            connectionString = connAnnotation.connectionString();
                            localSaveKeys = connAnnotation.saveKeys();
                        }
                    }
                }
                // detect table name from class if it is not already present
                if (tableName == null || tableName.isEmpty()) {
                    tableName = className;
                    tableName = tableName.substring(tableName.lastIndexOf(".") + 1);
                    if (!tableName.endsWith("s")) {
                        tableName = tableName.concat("s");
                    }
                }
                tableIndex = addTableToCache(className, tableName);
                sqlDriverClasses[tableIndex] = sqlDriverClass;
                connectionStrings[tableIndex] = connectionString;
                saveKeys[tableIndex] = localSaveKeys;

                ArrayList<Field> al_fields = new ArrayList<Field>();
                ArrayList<String> al_attributes = new ArrayList<String>();
                ArrayList<Boolean> al_attributes_notnull = new ArrayList<Boolean>();
                ArrayList<String> al_attributes_types = new ArrayList<String>();
                ArrayList<Integer> al_attributes_lengths = new ArrayList<Integer>();
                field:
                fields:for (Field field : clazz.getDeclaredFields()) {
                    String name = "";
                    Integer length = -1;
                    boolean notNull = false;
                    String sqlType = "";
                    Annotation[] annotations = field.getAnnotations();
                    for (Annotation annotation : annotations) {
                        if (annotation instanceof Column) {
                            if(((Column) annotation).ignore()) {
                                continue fields;
                            }
                            name = ((Column) annotation).name();
                            length = ((Column) annotation).length();
                            notNull = ((Column) annotation).notNull();
                            sqlType = ((Column) annotation).sqlType();
                            break;
                        }
                    }
                    field.setAccessible(true);
                    al_fields.add(field);
                    String javaType = field.getType().isPrimitive() ? field.getType().getCanonicalName() : getPrimitiveType(field.getType()).getCanonicalName();
                    if (name.isEmpty()) {
                        name = field.getName();
                    }
                    if (length < 0) {
                        length = getSQLTypeLengthFromJava(javaType);
                    }
                    if (sqlType.isEmpty()) {
                        sqlType = getSQLTypeFromJava(javaType);
                    }
                    al_attributes.add(name);
                    al_attributes_notnull.add(notNull);
                    al_attributes_types.add(sqlType);
                    al_attributes_lengths.add(length);
                }
                attributes[tableIndex] = al_attributes.toArray(new String[0]);
                attributeTypes[tableIndex] = al_attributes_types.toArray(new String[0]);
                attributeLengths[tableIndex] = al_attributes_lengths.toArray(new Integer[0]);
                attributeNotNulls[tableIndex] = al_attributes_notnull.toArray(new Boolean[0]);
                fields = al_fields.toArray(new Field[0]);

                ArrayList<String> al_keys = new ArrayList<String>();
                ArrayList<Integer> al_keyPos = new ArrayList<Integer>();
                for (int i = 0; i < attributes[tableIndex].length; i++) {
                    if (clazz != null) {
                        try {
                            Field field = fields[i];
                            Annotation[] annotations = field.getAnnotations();
                            for (Annotation annotation : annotations) {
                                if (annotation instanceof Key) {
                                    al_keys.add(attributes[tableIndex][i]);
                                    al_keyPos.add(i);
                                }
                            }
                        } catch (SecurityException ex) {
                        }
                    }
                }
                keys[tableIndex] = al_keys.toArray(new String[0]);
                keyPos[tableIndex] = al_keyPos.toArray(new Integer[0]);
                defaultValues[tableIndex] = new Object[attributes[tableIndex].length];
                mappings[tableIndex] = new Many[attributes[tableIndex].length][];

                Object o = null;
                try {
                    o = clazz.newInstance();
                } catch (InstantiationException ex) {
                } catch (IllegalAccessException ex) {
                } catch (IllegalArgumentException ex) {
                } catch (SecurityException ex) {
                }

                for (int i = 0; i < attributes[tableIndex].length; i++) {
                    defaultValues[tableIndex][i] = null;
                    if (clazz != null) {
                        try {
                            for (Field field : clazz.getDeclaredFields()) {
                                if (attributes[tableIndex][i].equals(field.getName()) || (field.getAnnotation(Column.class) != null && attributes[tableIndex][i].equals(field.getAnnotation(Column.class).name()))) {
                                    field.setAccessible(true);
                                    try {
                                        defaultValues[tableIndex][i] = field.get(o == null ? this : o);
                                    } catch (IllegalArgumentException ex) {
                                    } catch (IllegalAccessException ex) {
                                    }
                                    Annotation[] annotations = field.getAnnotations();
                                    for (Annotation annotation : annotations) {
                                        if (annotation instanceof Many) {
                                            if (!isKeyAttributeAtPos(i)) {
                                                throw new ezJDOException("Has and Many annotations can only be used on Primary id attributes.");
                                            }
                                            Many mapAnnotation = (Many) annotation;
                                            ArrayList<Many> temp = new ArrayList<Many>();
                                            temp.add(mapAnnotation);
                                            mappings[tableIndex][i] = temp.toArray(new Many[0]);
                                        } else if (annotation instanceof And) {
                                            if (!isKeyAttributeAtPos(i)) {
                                                throw new ezJDOException("Has and Many annotations can only be used on Primary id attributes.");
                                            }
                                            And mappingAnnotation = (And) annotation;
                                            ArrayList<Many> temp = new ArrayList<Many>();
                                            temp.addAll(Arrays.asList(mappingAnnotation.value()));
                                            mappings[tableIndex][i] = temp.toArray(new Many[0]);
                                        }
                                    }
                                    break;
                                }
                            }
                        } catch (SecurityException ex) {
                        }
                    }
                }
                // compare with database. and update database table schema
                if (sqlDriverClass != null && connectionString != null) {
                    try {
                        boolean tblexists = false;
                        DatabaseMetaData metaData = getConnection(tableIndex, Thread.currentThread().getId()).getMetaData();
                        dbName[tableIndex] = metaData.getDatabaseProductName();
                        //check to see if table exists
                        ResultSet rsTables = metaData.getTables(null, null, null, null);
                        while (rsTables.next()) {
                            if (getTableName().equalsIgnoreCase(rsTables.getString("TABLE_NAME"))) {
                                tblexists = true;
                                break;
                            }
                        }
                        rsTables.close();

                        if (tblexists) {
                            ArrayList<String> tal_attributes = new ArrayList<String>();
                            ArrayList<String> tal_attributes_types = new ArrayList<String>();
                            ArrayList<Integer> tal_attributes_lengths = new ArrayList<Integer>();
                            ArrayList<String> tal_keys = new ArrayList<String>();
                            ArrayList<Integer> tal_keyPos = new ArrayList<Integer>();

                            ResultSet rsColumns = metaData.getColumns(null, null, getTableName(), null);
                            while (rsColumns.next()) {
                                tal_attributes.add(rsColumns.getString("COLUMN_NAME"));
                                tal_attributes_types.add(rsColumns.getString("TYPE_NAME"));
                                tal_attributes_lengths.add(rsColumns.getInt("COLUMN_SIZE"));
                            }
                            rsColumns.close();

                            //keys
                            ResultSet rsKeys = metaData.getPrimaryKeys(null, null, getTableName());
                            while (rsKeys.next()) {
                                tal_keys.add(rsKeys.getString("COLUMN_NAME"));
                                tal_keyPos.add(getIndexForAttribute(tableIndex, rsKeys.getString("COLUMN_NAME")));
                            }
                            rsKeys.close();
                        } else {
                            log.log(Level.FINEST, "Table does not exist for {0} generating it from class definition.", className);
                            StringBuilder createStmt = new StringBuilder();
                            createStmt.append("CREATE TABLE ").append(getTableName()).append(" (");
                            for (int i = 0; i < attributes[tableIndex].length; i++) {
                                createStmt.append(attributes[tableIndex][i]).append(" ").append(attributeTypes[tableIndex][i]).append(" (").append(attributeLengths[tableIndex][i]).append(")");
                                if (attributeNotNulls[tableIndex][i]) {
                                    createStmt.append(" NOT NULL ");
                                }
                                if (defaultValues[tableIndex][i] != null) {
                                    createStmt.append(" DEFAULT ").append("'" + defaultValues[tableIndex][i] + "'");
                                }
                                if (i < attributes[tableIndex].length - 1) {
                                    createStmt.append(", ");
                                }
                            }
                            createStmt.append(");");
                            if (BaseObject.debugging) {
                                log.log(Level.INFO, createStmt.toString());
                            }
                            getConnection(tableIndex, Thread.currentThread().getId()).prepareStatement(createStmt.toString()).executeUpdate();

                            // add primary keys
                            for (int i = 0; i < keys[tableIndex].length; i++) {
                                alterAddPrimaryKey(keys[tableIndex][i], attributeTypes[tableIndex][i], attributeLengths[tableIndex][i], keys[tableIndex].length == 1 && isIntegerSQLType(attributeTypes[tableIndex][i]));
                            }
                            // add forien keys
                            // add contraints
                        }
                    } catch (SQLException ex) {
                        throw new ezJDOException("The method getColumnInfo() faild to retreive information about the table. Please check your class: \"" + className + "\" implementation.", ex);
                    }
                }
            }

            boolean skipFields = true;
            if (fields == null) {
                fields = new Field[attributes[tableIndex].length];
                skipFields = false;
            }
            originalValues = new Object[attributes[tableIndex].length];
            fieldsValid = new Boolean[attributes[tableIndex].length];
            fieldMessages = new String[attributes[tableIndex].length];
            for (int i = 0; i < attributes[tableIndex].length; i++) {
                originalValues[i] = defaultValues[tableIndex][i];
                fieldsValid[i] = true;
                fieldMessages[i] = null;
                if (!skipFields) {
                    try {
                        for (Field field : getClass().getDeclaredFields()) {
                            if (attributes[tableIndex][i].equals(field.getName()) || (field.getAnnotation(Column.class) != null && attributes[tableIndex][i].equals(field.getAnnotation(Column.class).name()))) {
                                fields[i] = field;
                                fields[i].setAccessible(true);
                                fields[i].set(this, defaultValues[tableIndex][i]);
                                break;
                            }
                        }
                    } catch (IllegalArgumentException ex) {
                    } catch (IllegalAccessException ex) {
                    } catch (SecurityException ex) {
                    }
                }
            }
        }
    }

    /* package */ static Connection getConnection(int tableIndex, long threadId) throws ezJDOException {
        Connection connection;
        HashMap<String, Connection> connectionstringConnections;
        synchronized (threadConnectionstringConnections) {
            connectionstringConnections = threadConnectionstringConnections.get(threadId);
        }
        if (connectionstringConnections != null) {
            connection = connectionstringConnections.get(connectionStrings[tableIndex]);
            if (connection == null) {
                connection = createConnection(tableIndex);
                connectionstringConnections.put(connectionStrings[tableIndex], connection);
            }
        } else {
            connectionstringConnections = new HashMap<String, Connection>();
            connection = createConnection(tableIndex);
            connectionstringConnections.put(connectionStrings[tableIndex], connection);
            synchronized (threadConnectionstringConnections) {
                threadConnectionstringConnections.put(threadId, connectionstringConnections);
            }
            final Thread curr = Thread.currentThread();
            new Thread() {
                @Override
                public void run() {
                    try {
                        log.log(Level.FINEST, " [+] connection thread open : {0}", curr.getId());
                        curr.join();
                        log.log(Level.FINEST, " [-] connection thread close : {0}", (curr != null ? curr.getId() : null));
                        if (curr != null) {
                            closeConnections(curr.getId());
                        }
                    } catch (InterruptedException ex) {
                        log.log(Level.FINEST, " [!] connection thread interrupted : " + (curr != null ? curr.getId() : null), ex);
                    }
                }
            }.start();
        }
        try {
            if (connection.isClosed()) {
                connection = createConnection(tableIndex);
                connectionstringConnections.put(connectionStrings[tableIndex], connection);
            }
        } catch (SQLException ex) {
        }
        return connection;
    }

    /* package */ static int getTableCacheIndex(String className) throws ezJDOException {
        for (int i = 0; i < classes.length; i++) {
            if (className.equals(classes[i])) {
                return i;
            }
        }
        try {
            StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
            StackTraceElement caller = stackTraceElements[2];
            if (!("chilliwebs.ezjdo.com.BaseObject".equals(caller.getClassName()) && "init".equals(caller.getMethodName()))) {
                Constructor c;
                Class clazz = Class.forName(className);
                try {
                    c = clazz.getDeclaredConstructor();
                    c.setAccessible(true);
                    c.newInstance();
                    for (int i = 0; i < classes.length; i++) {
                        if (className.equals(classes[i])) {
                            return i;
                        }
                    }
                } catch (NoSuchMethodException ex) {
                    if (BaseObject.class.isAssignableFrom(clazz)) {
                        throw new ezJDOException("There was a problem building the table cache for \"" + className + "\".", ex);
                    }
                }
            }
        } catch (InstantiationException ex) {
            throw new ezJDOException("There was a problem building the table cache for \"" + className + "\".", ex);
        } catch (IllegalAccessException ex) {
            throw new ezJDOException("There was a problem building the table cache for \"" + className + "\".", ex);
        } catch (IllegalArgumentException ex) {
            throw new ezJDOException("There was a problem building the table cache for \"" + className + "\".", ex);
        } catch (InvocationTargetException ex) {
            throw new ezJDOException("There was a problem building the table cache for \"" + className + "\".", ex);
        } catch (SecurityException ex) {
            throw new ezJDOException("There was a problem building the table cache for \"" + className + "\".", ex);
        } catch (ClassNotFoundException ex) {
            throw new ezJDOException("There was a problem building the table cache for \"" + className + "\".", ex);
        }
        return -1;
    }

    private static Connection createConnection(int tableIndex) throws ezJDOException {
        Connection connection = null;
        try {
            Pattern pattern = Pattern.compile("^jdbc:\\w+://.*$", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(connectionStrings[tableIndex]);
            if (matcher.find()) {
                Class.forName(sqlDriverClasses[tableIndex]);
                connection = DriverManager.getConnection(connectionStrings[tableIndex]);
            } else if (System.getProperty(connectionStrings[tableIndex]) != null) {
                Class.forName(sqlDriverClasses[tableIndex]);
                connection = DriverManager.getConnection(System.getProperty(connectionStrings[tableIndex]));
            } else if (new File("ezJDO.properties").exists()) {
                Properties propertiesFile = new Properties();
                try {
                    FileInputStream fis = new FileInputStream("ezJDO.properties");
                    propertiesFile.load(fis);
                    fis.close();
                    Class.forName(sqlDriverClasses[tableIndex]);
                    connection = DriverManager.getConnection(propertiesFile.getProperty(connectionStrings[tableIndex]));
                } catch (IOException ex) {
                    throw new ezJDOException("The ezJDO.properties file had issues  that matches the conection string value.");
                }
            } else {
                throw new ezJDOException("Invalid connection string value. Please make sure you have specified a valid connectrion string, or that there is a system property or ezJDO.properties that matches the conection string value.");
            }
        } catch (SQLException ex) {
            throw new ezJDOException("Cannot connect to the database. Check your database connection and the class variables for connectionStrings, serverName and databaseName.", ex);
        } catch (ClassNotFoundException ex) {
            throw new ezJDOException("The sqlDriverClasses variable is incorrect, or the jar library is not loaded.", ex);
        }
        return connection;
    }

    private static void closeConnections(long threadId) {
        HashMap<String, Connection> conns;
        synchronized (threadConnectionstringConnections) {
            conns = threadConnectionstringConnections.remove(threadId);
        }
        if (conns != null) {
            for (Connection con : conns.values()) {
                try {
                    con.close();
                } catch (SQLException ex) {
                }
            }
        }
    }

    private static int addTableToCache(String className, String tableName) {
        int i = 0;
        for (; i < tables.length; i++) {
            if (tables[i] == null) {
                tables[i] = tableName;
                classes[i] = className;
                return i;
            }
        }
        // we need to grow the cache
        String[] temp14 = new String[dbName.length + 5];
        System.arraycopy(dbName, 0, temp14, 0, dbName.length);
        dbName = temp14;
        String[] temp = new String[tables.length + 5];
        System.arraycopy(tables, 0, temp, 0, tables.length);
        tables = temp;
        tables[i] = tableName;
        String[] temp2 = new String[classes.length + 5];
        System.arraycopy(classes, 0, temp2, 0, classes.length);
        classes = temp2;
        classes[i] = className;
        String[] temp3 = new String[sqlDriverClasses.length + 5];
        System.arraycopy(sqlDriverClasses, 0, temp3, 0, sqlDriverClasses.length);
        sqlDriverClasses = temp3;
        String[] temp4 = new String[connectionStrings.length + 5];
        System.arraycopy(connectionStrings, 0, temp4, 0, connectionStrings.length);
        connectionStrings = temp4;
        Boolean[] temp10 = new Boolean[saveKeys.length + 5];
        System.arraycopy(saveKeys, 0, temp10, 0, saveKeys.length);
        saveKeys = temp10;
        Integer[][] temp13 = new Integer[attributeLengths.length + 5][];
        System.arraycopy(attributeLengths, 0, temp13, 0, attributeLengths.length);
        attributeLengths = temp13;
        Boolean[][] temp15 = new Boolean[attributeNotNulls.length + 5][];
        System.arraycopy(attributeNotNulls, 0, temp15, 0, attributeNotNulls.length);
        attributeNotNulls = temp15;
        String[][] temp5 = new String[attributes.length + 5][];
        System.arraycopy(attributes, 0, temp5, 0, attributes.length);
        attributes = temp5;
        String[][] temp12 = new String[attributeTypes.length + 5][];
        System.arraycopy(attributeTypes, 0, temp12, 0, attributeTypes.length);
        attributeTypes = temp12;
        String[][] temp6 = new String[keys.length + 5][];
        System.arraycopy(keys, 0, temp6, 0, keys.length);
        keys = temp6;
        Integer[][] temp7 = new Integer[keyPos.length + 5][];
        System.arraycopy(keyPos, 0, temp7, 0, keyPos.length);
        keyPos = temp7;
        Object[][] temp8 = new Object[defaultValues.length + 5][];
        System.arraycopy(defaultValues, 0, temp8, 0, defaultValues.length);
        defaultValues = temp8;
        Many[][][] temp9 = new Many[mappings.length + 5][][];
        System.arraycopy(mappings, 0, temp9, 0, mappings.length);
        mappings = temp9;
        return i;
    }

    private static StackTraceElement getCaller() {
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        StackTraceElement caller = stackTraceElements[3];
        return caller;
    }

    private static String join(Object[] objects, String glue) {
        int k = objects.length;
        if (k == 0) {
            return "";
        }
        StringBuilder out = new StringBuilder();
        out.append(objects[0]);
        for (int x = 1; x < k; ++x) {
            out.append(glue).append(objects[x]);
        }
        return out.toString();
    }
}
