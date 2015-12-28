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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Nick Hecht chilliwebs@gmail.com
 */
/* package */ class MSSQLQueryHelper implements SQLQueryHelper {

    @Override
    public <T> Results<T> createPagedResults(Class<T> clazz, String originalSQL, Object[] originalValues, Integer pageNumber, Integer itemsPerPage) throws ezJDOException {
        String[] tableKeys = BaseObject.getTableKeys(BaseObject.getClassTableName(clazz.getName()));
        Object[] newValues;
        if (pageNumber == 1) {
            newValues = originalValues;
        } else {
            if (originalValues.length > 0) {
                newValues = new Object[originalValues.length * 2];
                System.arraycopy(originalValues, 0, newValues, 0, originalValues.length);
                System.arraycopy(originalValues, 0, newValues, originalValues.length, originalValues.length);
            } else {
                newValues = originalValues;
            }
        }
        String tmpSQL = (" " + originalSQL.replaceAll(";", " ") + " ").toLowerCase();
        Pattern pattern = Pattern.compile("([\\s;]+select\\s)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(tmpSQL);
        int selectCount;
        for (selectCount = 0; matcher.find(); selectCount++);
        if (selectCount == 1 && !(tmpSQL.contains(" update ") || tmpSQL.contains(" insert ") || tmpSQL.contains(" delete "))) {
            tmpSQL = originalSQL.toLowerCase();
            int sel = tmpSQL.indexOf("select");
            int frm = tmpSQL.lastIndexOf("from");
            int whr = tmpSQL.lastIndexOf("where");
            int grp = tmpSQL.lastIndexOf("group by");
            int ord = tmpSQL.lastIndexOf("order by");
            if (sel >= 0 && frm > sel) {
                tmpSQL = originalSQL;
                // break statment apart
                String order = "";
                if (ord > 0) {
                    order = tmpSQL.substring(ord).replace(";", "").trim();
                    tmpSQL = tmpSQL.substring(0, ord).trim();
                }
                String[] order_feilds = new String[0];
                if (!order.trim().isEmpty()) {
                    String order_feilds_str = "";
                    order_feilds_str = order.replaceAll("\\s[aA|dD][eE]?[sS][cC]", "").replaceAll("[oO][rR][dD][eE][rR]\\s+[bB][yY]\\s", "");
                    order_feilds = order_feilds_str.split(",");
                }
                if (order_feilds.length < 1) {
                    if (tableKeys.length < 1) {
                        order_feilds = new String[]{"id"};
                    } else {
                        order_feilds = tableKeys;
                    }
                }
                String group = "";
                if (grp > 0) {
                    group = tmpSQL.substring(grp).replace(";", "").trim();
                    tmpSQL = tmpSQL.substring(0, grp).trim();
                }
                String where = "";
                if (whr > 0) {
                    where = tmpSQL.substring(whr + 5).replace(";", "").trim();
                    tmpSQL = tmpSQL.substring(0, whr).trim();
                }
                String table = "";
                if (frm > 0) {
                    table = tmpSQL.substring(frm + 4).replace(";", "").trim();
                    tmpSQL = tmpSQL.substring(0, frm).trim();
                }
                String fields = tmpSQL.substring(sel + 6).trim();
                if (group.trim().isEmpty()) {
                    String newSQL = "";
                    if (pageNumber == 1) {
                        newSQL = "SELECT TOP " + itemsPerPage + " " + fields + " FROM " + table + (("".equals(where)) ? "" : " WHERE (" + where + ")") + (("".equals(order)) ? "" : " " + order);
                    } else {
                        //SELECT TOP 5 * FROM ADM50_NOD_HELPA_MU WHERE ISN = '46' AND (CONVERT(varchar(5), ISN) + ' ' + CONVERT(varchar(5), MU$ORDER)) NOT IN (SELECT top 5 (CONVERT(varchar(5), ISN) + ' ' + CONVERT(varchar(5), MU$ORDER)) AS compare FROM ADM50_NOD_HELPA_MU WHERE ISN = '46')
                        String compare = "";
                        if (tableKeys.length > 1) {
                            compare = "(";
                            for (int i = 0; i < tableKeys.length; i++) {
                                compare += "CONVERT(varchar, " + tableKeys[i] + ")" + (((i + 1) < tableKeys.length) ? " + ' ' + " : "");
                            }
                            compare += ")";
                        } else {
                            compare = tableKeys[0];
                        }

                        newSQL = "SELECT TOP " + itemsPerPage + " " + fields + " FROM " + table + " WHERE " + (("".equals(where)) ? "" : "(" + where + ") AND ") + compare + " NOT IN ( SELECT TOP " + ((pageNumber - 1) * itemsPerPage) + " " + compare + " FROM " + table + " WHERE " + (("".equals(where)) ? "" : where) + (("".equals(order)) ? "" : " " + order) + " ) " + (("".equals(order)) ? "" : " " + order);
                    }
                    return BaseObject.sql(clazz, newSQL, newValues);
                } else {
                    throw new com.chilliwebs.ezjdo.exceptions.ezJDOException("You cannot page querys with \"group by\" statments.");
                }
            }
        }
        return BaseObject.sql(clazz, originalSQL, originalValues);
    }
}
