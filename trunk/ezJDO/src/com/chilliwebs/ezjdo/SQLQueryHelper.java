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

/**
 * @author  Nick Hecht chilliwebs@gmail.com
 */
/* package */ interface SQLQueryHelper {

    public abstract <T> Results<T> createPagedResults(Class<T> clazz, String originalSQL, Object[] originalValues, Integer pageNumber, Integer itemsPerPage) throws ezJDOException;
}
