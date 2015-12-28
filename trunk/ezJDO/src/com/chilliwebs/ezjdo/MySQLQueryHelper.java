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
/* package */ class MySQLQueryHelper implements SQLQueryHelper {

    @Override
    public <T> Results<T> createPagedResults(Class<T> clazz, String originalSQL, Object[] originalValues, Integer pageNumber, Integer itemsPerPage) throws ezJDOException {
        String tmpSQL = (" " + originalSQL.replaceAll(";", " ") + " ").toLowerCase();
        Pattern pattern = Pattern.compile("([\\s;]+select\\s)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(tmpSQL);
        int selectCount;
        for (selectCount = 0; matcher.find(); selectCount++);
        if (selectCount == 1 && !(tmpSQL.contains(" update ") || tmpSQL.contains(" insert ") || tmpSQL.contains(" delete "))) {
            tmpSQL = originalSQL.toLowerCase();
            int sel = tmpSQL.indexOf("select");
            int frm = tmpSQL.lastIndexOf("from");
            int grp = tmpSQL.lastIndexOf("group by");
            int ord = tmpSQL.lastIndexOf("order by");
            if (sel >= 0 && frm > sel) {
                tmpSQL = originalSQL;
                // break statment apart
                if (ord > 0) {
                    tmpSQL = tmpSQL.substring(0, ord).trim();
                }
                String group = "";
                if (grp > 0) {
                    group = tmpSQL.substring(grp).replace(";", "").trim();
                }
                if (group.trim().isEmpty()) {
                    String newSQL = originalSQL + " LIMIT " + ((pageNumber - 1) * itemsPerPage) + "," + itemsPerPage;
                    return BaseObject.sql(clazz, newSQL, originalValues);
                } else {
                    throw new com.chilliwebs.ezjdo.exceptions.ezJDOException("You cannot page querys with \"group by\" statments.");
                }
            }
        }
        return BaseObject.sql(clazz, originalSQL, originalValues);
    }
}
