package org.apache.flink.table.operations.ddl;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Operation to describe a CREATE TABLE statement.
 */

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-09-18
 * @Time 11:04
 */
public class CreateTableOperation implements CreateOperation {

    private final ObjectIdentifier tableIdentifier;
    private CatalogTable catalogTable; //这里实例 一般是 org.apache.flink.table.catalog.CatalogTableImpl， 还有 建表的 schema，with xxx 等信息
    private boolean ignoreIfExists;
    private boolean isTemporary;

    public CreateTableOperation(
            ObjectIdentifier tableIdentifier,
            //ObjectIdentifier 就是 catalogName， databaseName， tableName 简单类
            CatalogTable catalogTable,
            boolean ignoreIfExists,
            boolean isTemporary) {
        this.tableIdentifier = tableIdentifier;
        this.catalogTable = catalogTable;
        this.ignoreIfExists = ignoreIfExists;
//        this.isTemporary = true; // 如果这里是 true 的话，sql create table 都是 零时表，不会出现在 当前的catalog 中
        this.isTemporary = isTemporary;
    }

    public CatalogTable getCatalogTable() {
        return catalogTable;
    }

    public ObjectIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public boolean isIgnoreIfExists() {
        return ignoreIfExists;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("catalogTable", catalogTable.toProperties());
        params.put("identifier", tableIdentifier);
        params.put("ignoreIfExists", ignoreIfExists);
        params.put("isTemporary", isTemporary);

        return OperationUtils.formatWithChildren(
                "CREATE TABLE",
                params,
                Collections.emptyList(),
                Operation::asSummaryString);
    }
}

