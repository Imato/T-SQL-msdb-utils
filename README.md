## Utils for msdb administration

### Monitoring job steps

Send error information about jobs steps, not whole job state. If one of steps was failure you will receive error information by e-mail.

1. Create procedure monitor_job_steps
2. Create and setup job msdb_monitoring
