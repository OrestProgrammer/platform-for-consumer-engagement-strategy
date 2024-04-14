IF NOT EXISTS (
    SELECT *
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'source_data_ingestion_assets_info'
)
BEGIN
    CREATE TABLE dbo.source_data_ingestion_assets_info (
        id INT PRIMARY KEY IDENTITY(1,1),
        group_name VARCHAR(255) NOT NULL,
        ingestion_directory VARCHAR(255) NOT NULL,
        storage_account VARCHAR(255) NOT NULL,
        container_name VARCHAR(255) NOT NULL,
        bronze_catalog VARCHAR(255) NOT NULL,
        bronze_layer VARCHAR(255) NOT NULL,
        bronze_table_name VARCHAR(255) NOT NULL,
        pii_columns VARCHAR(MAX) NULL,
        file_format VARCHAR(255) NOT NULL
    );
END;

GO

IF NOT EXISTS (
    SELECT *
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'activity_run_log'
)
BEGIN
    CREATE TABLE dbo.activity_run_log (
        id INT PRIMARY KEY IDENTITY(1,1),
        activity_log_timestamp DATETIME NOT NULL,
        log_status VARCHAR(100) NOT NULL,
        datafactory_name VARCHAR(255) NOT NULL,
        pipeline_name VARCHAR(255) NOT NULL,
        activity_name VARCHAR(255) NOT NULL,
        run_id VARCHAR(255) NOT NULL
    );
END;

GO