CREATE OR ALTER PROCEDURE [dbo].[INSERT_ACTIVITY_LOG] (@STATUS VARCHAR(100), @DATAFACTORY_NAME VARCHAR(255), @PIPELINE_NAME VARCHAR(255), @ACTIVITY_NAME VARCHAR(255), @RUN_ID VARCHAR(100))
AS
BEGIN
  INSERT INTO dbo.activity_run_log(
    activity_log_timestamp,
    log_status,
    datafactory_name,
    pipeline_name,
    activity_name,
    run_id
  )
  VALUES(
    CURRENT_TIMESTAMP,
    @STATUS,
    @DATAFACTORY_NAME,
    @PIPELINE_NAME,
    @ACTIVITY_NAME,
    @RUN_ID
  )
END;