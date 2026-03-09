CALL MONITORING.LOGS.SP_START_JOB_AUDIT(
    P_SOLUTION_NAME      => 'SAPP07',
    P_JOB_NAME           => %(_job_name)s,
    P_ENVIRONMENT        => CURRENT_WAREHOUSE(),
    P_ORCHESTRATOR_NAME  => %(_job_scheduler)s,
    P_ORCHESTRATOR_RUN_ID=> %(_run_id)s,
    P_TRIGGER_TYPE       => 'MANUAL',
    P_PARAMETERS         => NULL
);