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
 * Indicates a custom column name for a object member.
 *
 * <p>Because annotations are processable when the object is being instantiated
 * it makes using annotations for default values the best way to set up the
 * object, this is because field values are not set before the constructor
 * executes therefore java reflections will not suffice.
 *
 * @author Nick Hecht chilliwebs@gmail.com
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Column {

    /**
     * the name attribute for the annotation.
     */
    String name() default "";
    
    /**
     * the length attribute for the annotation.
     */
    int length() default -1;
    
    /**
     * the notNull attribute for the annotation.
     */
    boolean notNull() default false;
    
    /**
     * the sqlType attribute for the annotation.
     */
    String sqlType() default "";
    
    /**
     * mark this attribute as not a database column.
     */
    boolean ignore() default false;
}
