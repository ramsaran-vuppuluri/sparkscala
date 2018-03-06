public final class SparkIOEnum {
    public enum Format {
        JDBC("jdbc"),
        CSV("csv");
        private String format;

        Format(String format) {
            this.format = format;
        }

        public String getFormat() {
            return format;
        }
    }

    public enum ReadMode {
        PERMISSIVE("permissive"),
        DROP_MALFORMED("dropMalformed"),
        FAIL_FAST("failFast"),
        DEFAULT_VALUE("failFast");

        private String readMode;

        ReadMode(String readModeIn) {
            this.readMode = readModeIn;
        }

        public String getReadMode() {
            return this.readMode;
        }
    }

    public enum WriteMode {
        APPEND("append"),
        OVER_WRITE("overwrite"),
        ERROR_IF_EXISTS("errorIfExists"),
        IGNORE("ignore"),
        DEFAULT_VALUE("errorIfExists");

        private String writeMode;

        WriteMode(String writeModeIn) {
            this.writeMode = writeModeIn;
        }

        public String getWriteMode() {
            return writeMode;
        }
    }


    public enum SQLOptions {
        URL("url", "The JDBC URL to which to connect. The source-specific connection properties can be specified in the URL; for example, jdbc:postgresql://localhost/test?user=fred&password=secret."),
        DB_TABLE("dbtable", "The JDBC table to read. Note that anything that is valid in a FROM clause of a SQL query can be used. For example, instead of a full table you could also use a subquery in parentheses."),
        DRIVER("driver", "The class name of the JDBC driver to use to connect to this URL."),
        PARTITION_COLUMN("partitionColumn", "These options must all be specified if any of them are specified. In addition, numPartitions must be specified. These properties describe how to partition the table when reading in parallel from multiple workers. partitionColumn must be a numeric column from the table in question. Notice that lowerBound and upperBound are used only to decide the partition stride, not for filtering the rows in the table. Thus, all rows in the table will be partitioned and returned. This option applies only to reading."),
        LOWER_BOUND("lowerBound", "These options must all be specified if any of them are specified. In addition, numPartitions must be specified. These properties describe how to partition the table when reading in parallel from multiple workers. partitionColumn must be a numeric column from the table in question. Notice that lowerBound and upperBound are used only to decide the partition stride, not for filtering the rows in the table. Thus, all rows in the table will be partitioned and returned. This option applies only to reading."),
        UPPER_BOUND("upperBound", "These options must all be specified if any of them are specified. In addition, numPartitions must be specified. These properties describe how to partition the table when reading in parallel from multiple workers. partitionColumn must be a numeric column from the table in question. Notice that lowerBound and upperBound are used only to decide the partition stride, not for filtering the rows in the table. Thus, all rows in the table will be partitioned and returned. This option applies only to reading."),
        NUM_PARTITIONS("numPartitions", "The maximum number of partitions that can be used for parallelism in table reading and writing. This also determines the maximum number of concurrent JDBC connections. If the number of partitions to write exceeds this limit, we decrease it to this limit by calling coalesce(numPartitions) before writing."),
        FETCH_SIZE("fetchsize", "The JDBC fetch size, which determines how many rows to fetch per round trip. This can help performance on JDBC drivers, which default to low fetch size (e.g., Oracle with 10 rows). This option applies only to reading."),
        BATCH_SIZE("batchsize", "The JDBC batch size, which determines how many rows to insert per round trip. This can help performance on JDBC drivers. This option applies only to writing. The default is 1000."),
        ISOLATION_LEVEL("isolationLevel", "The transaction isolation level, which applies to current connection. It can be one of NONE, READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, or SERIALIZABLE, corresponding to standard transaction isolation levels defined by JDBC’s Connection object. The default is READ_UNCOMMITTED. This option applies only to writing. For more information, refer to the documentation in java.sql.Connection."),
        TRUNCATE("truncate", "This is a JDBC writer-related option. When SaveMode.Overwrite is enabled, Spark truncates an existing table instead of dropping and re-creating it. This can be more efficient, and it prevents the table metadata (e.g., indices) from being removed. However, it will not work in some cases, such as when the new data has a different schema. The default is false. This option applies only to writing."),
        CREATE_TABLE_OPTIONS("createTableOptions", "This is a JDBC writer-related option. If specified, this option allows setting of database-specific table and partition options when creating a table (e.g., CREATE TABLE t (name string) ENGINE=InnoDB). This option applies only to writing."),
        CREATE_TABLE_COLUMN_TYPES("createTableColumnTypes", "The database column data types to use instead of the defaults, when creating the table. Data type information should be specified in the same format as CREATE TABLE columns syntax (e.g: “name CHAR(64), comments VARCHAR(1024)”). The specified types should be valid Spark SQL data types. This option applies only to writing.");

        private String sqlOption;
        private String sqlOptionInfo;

        SQLOptions(String sqlOptionIn, String sqlOptionInfoIn) {
            this.sqlOption = sqlOptionIn;
            this.sqlOptionInfo = sqlOptionInfoIn;
        }

        public String getSqlOption() {
            return this.sqlOption;
        }

        public String getSqlOptionInfo() {
            return this.sqlOptionInfo;
        }
    }
}