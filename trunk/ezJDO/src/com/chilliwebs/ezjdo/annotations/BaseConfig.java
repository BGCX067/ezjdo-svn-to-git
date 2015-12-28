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
package com.chilliwebs.ezjdo.annotations;

import java.lang.annotation.*;

/**
 * Indicates how the object makes a connection to the database and the default
 * table name to use.
 *
 * <p>Both sqlDriverClass and connectionString are required, but table name is
 * not, this is because the table name is generated from the object class name.
 *
 * @author Nick Hecht chilliwebs@gmail.com
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface BaseConfig {
    
    /**
     * the saveKeys attribute for the annotation.
     */
    boolean saveKeys() default false;
    
    /**
     * the tableName attribute for the annotation.
     */
    String tableName() default "";

    /**
     * the sqlDriverClass attribute for the annotation.
     */
    String sqlDriverClass();

    /**
     * the connectionString attribute for the annotation.
     */
    String connectionString();
}
