use msdb
go

create table dbo.BackupSettings
(name varchar(255) not null, 
value varchar(max) not null,
orderBy smallint not null default (0),
constraint PK_BackupSettings primary key (name))
go

insert into dbo.BackupSettings (name, value)
values 
('Log Dirrecroty', 'E:\backup\sql\log'),
('Diff Dirrecroty', 'E:\backup\sql\diff'),
('Full Dirrecroty', 'E:\backup\sql\full')
go

insert into dbo.BackupSettings (name, value)
values 
('Full Backup Day', '1')
go

insert into dbo.BackupSettings (name, value)
values 
('Last Full Backup', '1900-01-01')
go


create proc dbo.CreateDir 
	@dir varchar(255)
as
begin 
	set nocount on;
	declare @cmd varchar(2000) = 'mkdir ' + @dir;
	exec xp_cmdshell @cmd, no_output;
end
go


create proc dbo.CreateBackup
  @debug bit = 0
as
begin
  set nocount on;

  declare @error varchar(max),
          @sql nvarchar(max),
          @tmp nvarchar(max),
          @db varchar(255),
          @day int = datepart(weekday, getdate()),
          @dir varchar(max),
          @date datetime = getdate(),
          @mode smallint;

  if
    exists (select top 1 1
              from dbo.BackupSettings
              where name = 'Full Backup Day'
                and value = cast(@day as varchar(10)))
    or exists (select top 1 1
                from dbo.BackupSettings
                where name = 'Last Full Backup'
                  and cast(value as datetime) + 7 < getdate())
  begin
    select @dir = value 
      from dbo.BackupSettings
      where name = 'Full Dirrecroty';

    set @tmp = 'BACKUP DATABASE [%s] TO  DISK = N''%s\%s_full_%i.bak'' WITH NOFORMAT, INIT,  NAME = N''%s-Full Database Backup %i'', SKIP, NOREWIND, NOUNLOAD, COMPRESSION,  STATS = 10';
    select @day = cast(value as int)
      from dbo.BackupSettings
      where name = 'Full Backup Day'

    set @mode = 1;
  end
  else 
  begin
    select @dir = value 
      from dbo.BackupSettings
      where name = 'Diff Dirrecroty';

    set @tmp = 'BACKUP DATABASE [%s] TO  DISK = N''%s\%s_diff_%i.bak'' WITH  DIFFERENTIAL , NOFORMAT, INIT,  NAME = N''%s-Diff Database Backup %i'', SKIP, NOREWIND, NOUNLOAD, COMPRESSION,  STATS = 10';

    set @mode = 2;
  end;
   
	exec dbo.CreateDir @dir;

  declare cur cursor for 
    select name
      from sys.databases d
      where d.state = 0
        and d.name not in ('model', 'tempdb');

  open cur;
  fetch next from cur into @db;

  while @@FETCH_STATUS = 0
  begin
    begin try
      set @sql = formatmessage(@tmp, @db, @dir, @db, @day, @db, @day);
      if @debug = 1 exec sp_print @sql;
      if @debug = 0 exec sp_executesql @sql;
    end try
    begin catch
      set @error += ERROR_MESSAGE() + '
';
    end catch

    fetch next from cur into @db;
  end;
    
  close cur;
  deallocate cur;

  if len(@error) > 0
    throw 60000, @error, 1;

  if @mode = 1
    update dbo.BackupSettings
      set value = convert(varchar, @date, 121)
      where name = 'Last Full Backup'

end
go

create proc dbo.CreateBackupLog
  @debug bit = 0
as
begin
  set nocount on;

  declare @error varchar(max),
          @sql nvarchar(max),
          @tmp nvarchar(max),
          @db varchar(255),
          @dir varchar(max),
          @date varchar(30) = format(getdate(), 'yyyy-MM-dd-HH-mm-ss');

  select @dir = value 
    from dbo.BackupSettings
    where name = 'Log Dirrecroty';

  set @tmp = 'BACKUP LOG [%s] TO DISK=N''%s\%s-%s.trn'' WITH FORMAT, COMPRESSION';   
  
	exec dbo.CreateDir @dir;

  declare cur cursor for 
    select name
      from sys.databases d
      where d.state = 0
        and d.recovery_model = 1 
        and d.name not in ('model', 'tempdb');

  open cur;
  fetch next from cur into @db;

  while @@FETCH_STATUS = 0
  begin
    begin try
      set @sql = formatmessage(@tmp, @db, @dir, @db, @date);
      if @debug = 1 exec sp_print @sql;
      if @debug = 0 exec sp_executesql @sql;
    end try
    begin catch
      set @error += ERROR_MESSAGE() + '
';
    end catch

    fetch next from cur into @db;
  end;
    
  close cur;
  deallocate cur;

  if len(@error) > 0
    throw 60001, @error, 1;
end
go


-- exec dbo.CreateBackup 1;
-- exec dbo.CreateBackupLog 1;

-- jobs

EXEC msdb.dbo.sp_add_operator @name=N'SqlAdmins', 
		@enabled=1, 
		@weekday_pager_start_time=90000, 
		@weekday_pager_end_time=180000, 
		@saturday_pager_start_time=90000, 
		@saturday_pager_end_time=180000, 
		@sunday_pager_start_time=90000, 
		@sunday_pager_end_time=180000, 
		@pager_days=0, 
		@email_address=N'a.varentsov@corp.mail.ru', 
		@category_name=N'[Uncategorized]'
GO


BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0

IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Backup', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'SqlAdmins', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Backup]    Script Date: 10.03.2021 1:33:16 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Backup', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec dbo.CreateBackup', 
		@database_name=N'msdb', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'every_day_1', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20210310, 
		@active_end_date=99991231, 
		@active_start_time=10300, 
		@active_end_time=235959, 
		@schedule_uid=N'598025db-0311-4ea4-9557-395a416fe255'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0

IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'BackupLog', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'SqlAdmins', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Backup]    Script Date: 12.03.2021 19:37:33 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Backup', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec dbo.CreateBackupLog', 
		@database_name=N'msdb', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Clear]    Script Date: 12.03.2021 19:37:34 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Clear', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'PowerShell', 
		@command=N'Get-ChildItem E:\backup\sql\log | where LastWriteTime -lt (Get-Date).AddDays(-8) | Remove-Item', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'every_30_min', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=30, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20210310, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'1ad50240-c873-4509-b0d4-b0ce46009278'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


