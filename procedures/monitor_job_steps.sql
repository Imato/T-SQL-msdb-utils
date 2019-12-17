create proc dbo.monitor_job_steps
as
begin
  set nocount on;

  declare @monitor_period_sec int = 300,
          @max_job_duration_sec int = 1800,
          @message varchar(max) = '',
          @recipients varchar(2000),
          @subject varchar(255) = @@SERVERNAME + ' msdb monitoring',
          @str varchar(max) = '';

  select @recipients = o.email_address
    from dbo.sysoperators o
    where name = 'SqlAdmins';

  if (@recipients is null or len(@recipients) = 0)
    return;

  with steps
  as
  (select
      convert(datetime, cast(sjstp.last_run_date as varchar(8)), 112) +
      cast(stuff(stuff(right('000000' + cast(sjstp.last_run_time as varchar(6)),6),3,0,':'),6,0,':') as datetime) as last_run_date,
      sjob.name,
      sjstp.step_name,
      coalesce((select top 1 message
                  from msdb.dbo.sysjobhistory sub_jh
                  where sub_jh.step_id =sjstp.step_id
                    and sub_jh.job_id = sjstp.job_id
                    and sub_jh.run_status = 0
                  order by cast(sub_jh.run_date as varchar(10)) + cast(sub_jh.run_time as varchar(50)) desc),'') as error,
      sjstp.last_run_outcome
    from msdb.dbo.sysjobsteps as sjstp with(nolock)
    join msdb.dbo.sysjobs   as sjob  with(nolock) on sjstp.job_id = sjob.job_id
    where cast(sjstp.last_run_date as varchar(8)) + right('000000' + cast(sjstp.last_run_time as varchar(6)),  6)
      >= replace(replace(replace(convert(varchar(25),dateadd(second, -@monitor_period_sec, getdate()), 120),'-',''),':',''),' ','')
      and sjstp.step_id > 0
      and sjob.enabled = 1)

  select @str += '<div><b>' + format(s.last_run_date, 'yyyy-MM-dd HH:mm') + ': ' + s.name + '/' + s.step_name + '</b> - ' + isnull(s.error, '') + '</div>'
    from steps s
    where s.last_run_outcome = 0
      or len(s.error) > 0;

  if (len(@str) > 0)
    set @message = '<h3>Job steps with errors</h3>' + char(13) + @str + char(13);

  set @str = '';

  with steps
  as
  (select sjob.name job_name,
          sjstp.step_name step_name,
          convert(datetime, cast(sjstp.last_run_date as varchar(8)), 112) +
  cast(stuff(stuff(right('000000' + cast(sjstp.last_run_time as varchar(6)),6),3,0,':'),6,0,':') as datetime) as last_run_date,
          case sjstp.last_run_outcome
            when 0 then 'failed'
            when 1 then 'succeeded'
            when 2 then 'retry'
            when 3 then 'canceled'
            when 5 then 'unknown'
          end lastrun_state,
          stuff(stuff(right('000000' + cast(max(sjobhist.run_duration) as varchar(6)),6), 3, 0, ':'), 6, 0, ':') run_duration
      from msdb.dbo.sysjobhistory   sjobhist with(nolock)
        join msdb.dbo.sysjobs       sjob     with(nolock) on sjobhist.job_id = sjob.job_id
        join msdb.dbo.sysjobsteps   sjstp    with(nolock) on sjobhist.job_id = sjstp.job_id
          and sjobhist.step_id = sjstp.step_id
      where sjobhist.step_id > 0
        and sjob.enabled = 1
        and  cast(sjobhist.run_date as varchar(8)) + right('000000' + cast(sjobhist.run_time as varchar(6)),  6)
                  >= replace(replace(replace(convert(varchar(25),dateadd(second,-(@max_job_duration_sec * 5), getdate()),120),'-',''),':',''),' ','')
      group by sjob.name,
        sjstp.step_name,
        sjstp.last_run_outcome,
        sjstp.last_run_date,
        sjstp.last_run_time)

  select @str += '<div><b>' + format(s.last_run_date, 'yyyy-MM-dd HH:mm') + ': ' + s.job_name + '/' + s.step_name + '</b> - ' + cast(s.run_duration as varchar(10)) + '</div>'
    from steps s
    where datediff(SECOND,  '19000101', cast(s.run_duration as datetime)) > @max_job_duration_sec;

  if (len(@str) > 0)
    set @message += '<h3>Long job steps</h3>' + char(13) + @str + char(13);

  if (len(@message) > 0)
    exec dbo.sp_send_dbmail
      @profile_name = 'dwh-monitoring',
		  @recipients = @recipients,
		  @body = @message,
		  @subject = @subject,
		  @body_format = 'HTML';
end