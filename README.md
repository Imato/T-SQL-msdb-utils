## Utils for msdb administration

### Monitoring job steps

Send error information about jobs steps, not whole job state. If one of steps was failure you will receive error information by e-mail.

1. Create procedure monitor_job_steps
2. Create and setup job msdb_monitoring

### Simple backup strategy

- full backup for all DB every weeek
- differential backup every day
- backup log regularly

Run script backup_simple.sql, update created jobs name and settings (backup path in table msdb.dbo.BackupSettings and in job BackupLog.)
