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
 * Used for grouping multiple Many annotations together.
 *
 * <p>This will identify a relationships between this class and another so 
 * findMany() can be called to retrieve the related objects.
 *
 * @author Nick Hecht chilliwebs@gmail.com
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface And {

    /**
     * A collection of Many annotations.
     */
    Many[] value();
}
