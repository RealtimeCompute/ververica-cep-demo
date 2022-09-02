package com.alibaba.ververica.cep.demo;

public final class Constants {
    // Required configurations constants for connecting to JDBC
    public static final String JDBC_URL =
            "jdbc:mysql://${your_jdbc_link}:3306/${your_db_name}?user=${your_db_username}&password=${your_db_password}";
    public static final String JDBC_DRIVE = "com.mysql.cj.jdbc.Driver";
    public static final String TABLE_NAME = "${your_table_name}";
    public static final Long JDBC_INTERVAL_MILLIS = 5000L;
    // Required configurations constants for connecting to Kafka
    public static final String KAFKA_BROKERS = "${your_kafka_broker_url}";
    public static final String INPUT_TOPIC = "${your_kafka_topic}";
    public static final String INPUT_TOPIC_GROUP = "${your_kafka_topic_group}";
}
