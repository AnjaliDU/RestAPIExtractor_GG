CALL MONITORING.LOGS.SP_FINISH_JOB_AUDIT(
    P_AUDIT_ID  => %(_audit_id)s,
    P_STATUS_ID => %(status_id)s, --1 - NO ERROR, 2 - A LEAST ONE ERROR,
    P_NOTES     => %(error_message)s
);

