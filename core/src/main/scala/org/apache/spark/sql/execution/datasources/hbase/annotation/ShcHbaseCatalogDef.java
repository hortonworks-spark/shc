package org.apache.spark.sql.execution.datasources.hbase.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;


@Retention(RetentionPolicy.CLASS)
@Target({ElementType.LOCAL_VARIABLE, ElementType.METHOD})
public @interface ShcHbaseCatalogDef{
}

