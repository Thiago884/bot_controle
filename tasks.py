from datetime import datetime, timedelta
import asyncio
import logging
from main import bot
from discord.ext import tasks, commands
import discord
from io import BytesIO
from typing import Optional, Dict, List
from utils import generate_activity_graph
from collections import defaultdict
import time
import pytz
import json
import os

logger = logging.getLogger('inactivity_bot')

class PerformanceMetrics:
    def __init__(self):
        self.db_query_times = []
        self.api_call_times = []
        self.task_execution_times = [] 
        self.last_flush = time.time()
    
    def record_db_query(self, duration):
        self.db_query_times.append(duration)
        self._maybe_flush()
    
    def record_api_call(self, duration):
        self.api_call_times.append(duration)
        self._maybe_flush()
    
    def record_task_execution(self, task_name, duration):
        self.task_execution_times.append((task_name, duration))
        self._maybe_flush()
    
    def _maybe_flush(self):
        if time.time() - self.last_flush > 300:
            self._flush_metrics()
            self.last_flush = time.time()
    
    def _flush_metrics(self):
        if self.db_query_times:
            avg_db = sum(self.db_query_times) / len(self.db_query_times)
            max_db = max(self.db_query_times)
            logger.info(f"Métricas DB: Avg={avg_db:.3f}s, Max={max_db:.3f}s, Samples={len(self.db_query_times)}")
            self.db_query_times = []
        
        if self.api_call_times:
            avg_api = sum(self.api_call_times) / len(self.api_call_times)
            max_api = max(self.api_call_times)
            logger.info(f"Métricas API: Avg={avg_api:.3f}s, Max={max_api:.3f}s, Samples={len(self.api_call_times)}")
            self.api_call_times = []
        
        if self.task_execution_times:
            task_stats = defaultdict(list)
            for task_name, duration in self.task_execution_times:
                task_stats[task_name].append(duration)
            
            for task_name, durations in task_stats.items():
                avg = sum(durations) / len(durations)
                max_d = max(durations)
                logger.info(f"Métricas Task {task_name}: Avg={avg:.3f}s, Max={max_d:.3f}s, Samples={len(durations)}")
            
            self.task_execution_times = []

perf_metrics = PerformanceMetrics()

class TaskMetrics:
    def __init__(self):
        self.execution_times = defaultdict(list)
        self.error_counts = defaultdict(int)
        self.success_counts = defaultdict(int)
    
    def record_execution(self, task_name: str, duration: float):
        self.execution_times[task_name].append(duration)
        if len(self.execution_times[task_name]) > 100:
            self.execution_times[task_name].pop(0)
    
    def record_error(self, task_name: str):
        self.error_counts[task_name] += 1
    
    def record_success(self, task_name: str):
        self.success_counts[task_name] += 1
    
    def get_metrics(self, task_name: str) -> Dict:
        times = self.execution_times.get(task_name, [])
        return {
            'last_24h_errors': self.error_counts.get(task_name, 0),
            'last_24h_successes': self.success_counts.get(task_name, 0),
            'avg_time': sum(times)/len(times) if times else 0,
            'max_time': max(times) if times else 0,
        }

task_metrics = TaskMetrics()

def log_task_metrics(task_name: str):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                perf_metrics.record_task_execution(task_name, duration)
                task_metrics.record_success(task_name)
                return result
            except Exception as e:
                duration = time.time() - start_time
                perf_metrics.record_task_execution(task_name, duration)
                task_metrics.record_error(task_name)
                logger.error(f"Error in {task_name}: {e}", exc_info=True)
                raise
        return wrapper
    return decorator

async def execute_task_with_persistent_interval(task_name: str, task_func: callable, hours_interval: int = 24):
    """Executes a task maintaining a persistent interval in a robust way."""
    await bot.wait_until_ready()
    
    while not bot.db or not bot.db._is_initialized:
        await asyncio.sleep(5)
        logger.info(f"Waiting for database initialization for task {task_name}...")
    
    while True:
        try:
            last_exec_data = await bot.db.get_last_task_execution(task_name)
            now = datetime.now(pytz.UTC)
            should_execute = False

            if not last_exec_data:
                should_execute = True
                logger.info(f"First execution of task {task_name}")
            else:
                last_exec_time = last_exec_data['last_execution']
                if last_exec_time.tzinfo is None:
                    last_exec_time = last_exec_time.replace(tzinfo=pytz.UTC)
                
                next_scheduled_run = last_exec_time + timedelta(hours=hours_interval)
                
                if now >= next_scheduled_run:
                    should_execute = True
                else:
                    remaining_time = next_scheduled_run - now
                    logger.info(f"Task '{task_name}' scheduled. Next run in: {str(remaining_time).split('.')[0]}")

            if should_execute:
                logger.info(f"Executing task {task_name}...")
                start_time = time.time()
                await task_func()
                perf_metrics.record_task_execution(task_name, time.time() - start_time)

                await bot.db.log_task_execution(task_name, bot.config.get('monitoring_period', 14))
                logger.info(f"Task {task_name} completed successfully")
            
            await asyncio.sleep(3600)
                
        except Exception as e:
            logger.error(f"Error in task {task_name} cycle: {e}", exc_info=True)
            await asyncio.sleep(3600)

# ==============================================================================
# UNIFIED INACTIVITY MANAGER - THE NEW CENTRALIZED TASK
# ==============================================================================

@log_task_metrics("unified_inactivity_manager")
async def unified_inactivity_manager():
    """
    Unified task that manages the entire inactivity lifecycle:
    - Processes monitoring periods that ended while the bot was offline
    - Sends warnings (1st and 2nd) at the correct times for the current period
    - Removes roles from members who failed to meet requirements at the end of a period
    """
    await bot.wait_until_ready()
    
    if 'tracked_roles' not in bot.config or not bot.config['tracked_roles']:
        logger.info("No tracked roles defined - unified inactivity task ignored.")
        return
    
    logger.info("Starting unified inactivity management task...")
    
    processed_count = 0
    warnings_sent = {'first': 0, 'second': 0}
    roles_removed_count = 0

    for guild in bot.guilds:
        tracked_roles = bot.config['tracked_roles']
        members_with_tracked_roles = [
            member for member in guild.members 
            if any(role.id in tracked_roles for role in member.roles) and not member.bot
        ]

        for member in members_with_tracked_roles:
            try:
                result = await _process_member_inactivity_state(member)
                processed_count += 1
                if result.get('warning_sent'):
                    warnings_sent[result['warning_sent']] += 1
                if result.get('roles_removed'):
                    roles_removed_count += 1
                
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error processing inactivity state for {member.display_name} ({member.id}): {e}", exc_info=True)

    logger.info(
        f"Unified task completed. Members processed: {processed_count}, "
        f"Warnings sent: {warnings_sent}, Roles removed: {roles_removed_count}"
    )

async def _process_member_inactivity_state(member: discord.Member) -> Dict:
    """Processes the state of a single member, deciding what action to take."""
    guild = member.guild
    config = bot.config
    db = bot.db
    result = {'warning_sent': None, 'roles_removed': False}

    # 1. Check Whitelist
    if member.id in config['whitelist']['users'] or any(role.id in config['whitelist']['roles'] for role in member.roles):
        return result

    # 2. Get assignment date of oldest tracked role
    tracked_roles_ids = [r.id for r in member.roles if r.id in config['tracked_roles']]
    if not tracked_roles_ids:
        return result

    assignment_times = []
    for role_id in tracked_roles_ids:
        time = await db.get_role_assigned_time(member.id, guild.id, role_id)
        if time:
            assignment_times.append(time)
        else:
            await db.log_role_assignment(member.id, guild.id, role_id)
            logger.info(f"Assignment record created for {member.display_name} and role {role_id}. Will be processed next cycle.")
            return result
            
    if not assignment_times:
        return result

    start_of_monitoring = min(assignment_times)

    # 3. Determine current verification period and process past periods
    monitoring_period_days = timedelta(days=config['monitoring_period'])
    period_end = start_of_monitoring + monitoring_period_days
    now = datetime.now(pytz.UTC)

    # Loop to "catch up" to current time, processing periods that ended while bot was offline
    while period_end <= now:
        period_start = period_end - monitoring_period_days
        
        last_check = await db.get_last_period_check(member.id, guild.id)
        if last_check and last_check['period_start'] == period_start and last_check['meets_requirements']:
            period_end += monitoring_period_days
            continue

        meets_reqs = await _check_activity_for_period(member, period_start, period_end)
        await db.log_period_check(member.id, guild.id, period_start, period_end, meets_reqs)

        if not meets_reqs:
            logger.info(f"{member.display_name} failed requirements for period (past) {period_start.date()} to {period_end.date()}. Removing roles.")
            roles_to_remove = [r for r in member.roles if r.id in tracked_roles_ids]
            if roles_to_remove:
                await _remove_inactive_roles(member, roles_to_remove)
                result['roles_removed'] = True
            return result

        period_end += monitoring_period_days

    # 4. Logic for CURRENT period (that will end in the future)
    days_remaining = (period_end - now).days
    period_start = period_end - monitoring_period_days

    last_warning_data = await db.get_last_warning_in_period(member.id, guild.id, period_start)
    last_warning_type = last_warning_data[0] if last_warning_data else None

    # Send 2nd warning
    if 0 <= days_remaining <= config['warnings']['second_warning']:
        if last_warning_type not in ['second', 'final']:
            await bot.send_warning(member, 'second')
            result['warning_sent'] = 'second'
            return result
    
    # Send 1st warning
    elif config['warnings']['second_warning'] < days_remaining <= config['warnings']['first_warning']:
        if not last_warning_type:
            await bot.send_warning(member, 'first')
            result['warning_sent'] = 'first'
            return result
            
    return result

async def _check_activity_for_period(member: discord.Member, period_start: datetime, period_end: datetime) -> bool:
    """Checks if a member met activity requirements for a given period."""
    sessions = await bot.db.get_voice_sessions(member.id, member.guild.id, period_start, period_end)
    
    required_minutes = bot.config['required_minutes']
    required_days = bot.config['required_days']
    
    valid_days = set()
    if sessions:
        daily_duration = defaultdict(int)
        for session in sessions:
            day = session['join_time'].astimezone(pytz.utc).date()
            daily_duration[day] += session.get('duration', 0)
        
        for day, total_duration_seconds in daily_duration.items():
            if (total_duration_seconds / 60) >= required_minutes:
                valid_days.add(day)

    return len(valid_days) >= required_days

async def _remove_inactive_roles(member: discord.Member, roles_to_remove: List[discord.Role]):
    """Helper function to remove roles, log and notify."""
    try:
        await member.remove_roles(*roles_to_remove, reason="Inactivity")
        
        role_ids_removed = [r.id for r in roles_to_remove]
        await bot.db.log_removed_roles(member.id, member.guild.id, role_ids_removed)
        
        await bot.send_warning(member, 'final')
        
        log_message = f"Roles removed due to inactivity: {', '.join([r.name for r in roles_to_remove])}"
        await bot.log_action("Roles Removed", member, log_message)
        
        await bot.notify_roles(
            f"🚨 Roles removed from {member.mention} due to inactivity: " +
            ", ".join([f"`{r.name}`" for r in roles_to_remove])
        )

    except discord.Forbidden:
        logger.error(f"Insufficient permissions to remove roles from {member.display_name}")
        await bot.log_action("Error Removing Roles", member, "Insufficient permissions")
    except Exception as e:
        logger.error(f"Error removing roles from {member.display_name}: {e}")

async def start_unified_inactivity_manager():
    """Wrapper for the unified task with persistent interval."""
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "unified_inactivity_manager", 
        unified_inactivity_manager,
        hours_interval=24
    ), name='unified_inactivity_manager_wrapper')
    return task

# ==============================================================================
# KEPT TASKS - Independent logic that remains useful
# ==============================================================================

def prioritize_members(members: list[discord.Member]) -> list[discord.Member]:
    """Sorts members to process those most likely to be inactive first."""
    return sorted(
        members,
        key=lambda m: (
            m.joined_at.timestamp() if m.joined_at else 0,
            len(m.roles),
        ),
        reverse=False
    )

async def get_time_without_roles(member: discord.Member) -> Optional[timedelta]:
    """Gets how long the member has been without roles (except @everyone)"""
    try:
        if len(member.roles) > 1:
            return None
            
        last_role_removal = await bot.db.get_last_role_removal(member.id, member.guild.id)
        
        if last_role_removal and last_role_removal.get('removal_date'):
            removal_date = last_role_removal['removal_date']
            if removal_date.tzinfo is None:
                removal_date = removal_date.replace(tzinfo=pytz.UTC)
            return datetime.now(pytz.UTC) - removal_date
            
        if member.joined_at:
            joined_at = member.joined_at.replace(tzinfo=pytz.UTC) if member.joined_at.tzinfo is None else member.joined_at
            return datetime.now(pytz.UTC) - joined_at
            
        return None
    except Exception as e:
        logger.error(f"Error checking time without roles for {member}: {e}", exc_info=True)
        return None

@log_task_metrics("cleanup_members")
async def _cleanup_members():
    """Kicks members who have been without monitored roles for a configured period."""
    await bot.wait_until_ready()
    
    kick_after_days = bot.config.get('kick_after_days', 0)
    if kick_after_days <= 0:
        logger.info("Kicking members without roles disabled.")
        return

    logger.info("Starting cleanup of members without roles...")
    members_kicked = 0
    
    for guild in bot.guilds:
        members_to_check = [m for m in guild.members if len(m.roles) <= 1 and not m.bot]
        
        for member in members_to_check:
            try:
                if member.id in bot.config['whitelist']['users']:
                    continue

                time_without = await get_time_without_roles(member)
                if time_without and time_without.days >= kick_after_days:
                    
                    last_kick = await bot.db.get_last_kick(member.id, guild.id)
                    if last_kick and (datetime.now(pytz.UTC) - last_kick['kick_date']).days < kick_after_days:
                        continue

                    logger.info(f"Kicking {member.display_name} for being without roles for {time_without.days} days.")
                    await member.kick(reason=f"Without roles for more than {kick_after_days} days.")
                    await bot.db.log_kicked_member(member.id, guild.id, f"Without roles for more than {kick_after_days} days")
                    await bot.log_action("Member Kicked", member, f"Reason: Without roles for more than {kick_after_days} days")
                    await bot.notify_roles(f"👢 {member.mention} was kicked for being without roles for more than {kick_after_days} days")
                    members_kicked += 1
                    await asyncio.sleep(1)
            except discord.Forbidden:
                 await bot.log_action("Error Kicking", member, "Role hierarchy or insufficient permissions.")
            except Exception as e:
                logger.error(f"Error processing cleanup for member {member.display_name}: {e}")

    logger.info(f"Member cleanup completed. Members kicked: {members_kicked}")

async def start_cleanup_members():
    """Wrapper for the member cleanup task."""
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "cleanup_members", 
        _cleanup_members,
        hours_interval=24
    ), name='cleanup_members_wrapper')
    return task

@log_task_metrics("database_backup")
async def _database_backup():
    """Performs daily database backup."""
    await bot.wait_until_ready()
    
    if not bot.db or not bot.db._is_initialized:
        logger.error("Database not initialized - skipping backup")
        return
        
    try:
        if not hasattr(bot, 'db_backup') or bot.db_backup is None:
            from database import DatabaseBackup
            bot.db_backup = DatabaseBackup(bot.db)
        
        success = await bot.db_backup.create_backup()
        
        if success:
            await bot.log_action("Database Backup", None, "Daily backup completed successfully")
            logger.info("Database backup completed successfully")
        else:
            logger.error("Failed to create database backup")
    except Exception as e:
        logger.error(f"Error executing database backup: {e}")

async def start_database_backup():
    """Wrapper for the backup task."""
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "database_backup", 
        _database_backup,
        hours_interval=24
    ), name='database_backup_wrapper')
    return task

@log_task_metrics("cleanup_old_data")
async def _cleanup_old_data():
    """Cleans up old database data to maintain performance."""
    await bot.wait_until_ready()
    
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Database not initialized - skipping old data cleanup")
        return
    
    try:
        log_message = await bot.db.cleanup_old_data(days=60)
        await bot.log_action("Old Data Cleanup", None, log_message)
        
    except Exception as e:
        logger.error(f"Error cleaning old data: {e}")
        await bot.log_action(
            "Data Cleanup Error", 
            None, 
            f"Failed to clean old data: {str(e)}"
        )

async def start_cleanup_old_data():
    """Wrapper for the data cleanup task."""
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "cleanup_old_data",
        _cleanup_old_data,
        hours_interval=168 # Weekly
    ), name='cleanup_old_data_wrapper')
    return task

async def _execute_force_check(member: discord.Member):
    """Executes a forced inactivity check for a specific member"""
    try:
        guild = member.guild
        required_minutes = bot.config['required_minutes']
        required_days = bot.config['required_days']
        monitoring_period = bot.config['monitoring_period']
        
        period_end = datetime.now(pytz.UTC)
        period_start = period_end - timedelta(days=monitoring_period)
        
        meets_requirements = await _check_activity_for_period(member, period_start, period_end)
        
        await bot.db.log_period_check(member.id, guild.id, period_start, period_end, meets_requirements)
        
        sessions = await bot.db.get_voice_sessions(member.id, guild.id, period_start, period_end)
        valid_days = set()
        if sessions:
             for session in sessions:
                if session['duration'] >= required_minutes * 60:
                    valid_days.add(session['join_time'].date())

        return {
            'meets_requirements': meets_requirements,
            'valid_days': len(valid_days),
            'required_days': required_days,
            'sessions_count': len(sessions),
            'period_start': period_start,
            'period_end': period_end
        }
        
    except Exception as e:
        logger.error(f"Error in forced check for {member}: {e}")
        raise

# ==============================================================================
# VOICE EVENT PROCESSING TASKS
# ==============================================================================

async def voice_event_processor():
    """Processes voice events from the main queue"""
    await bot.wait_until_ready()
    while True:
        try:
            event = await bot.voice_event_queue.get()
            await bot._process_voice_batch([event])
            bot.voice_event_queue.task_done()
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error in voice event processor: {e}")
            await asyncio.sleep(1)

@log_task_metrics("check_current_voice_members")
async def check_current_voice_members():
    """Checks all voice channels and updates internal state, including ghost session cleanup."""
    await bot.wait_until_ready()

    try:
        logger.info("Starting check of members in voice channels...")
        
        # Clean up ghost sessions in memory
        active_session_keys = list(bot.active_sessions.keys())
        for key in active_session_keys:
            user_id, guild_id = key
            guild = bot.get_guild(guild_id)
            if not guild:
                del bot.active_sessions[key]
                logger.info(f"Removed session for guild {guild_id} (not found).")
                continue
            
            member = guild.get_member(user_id)
            if not member or not member.voice or not member.voice.channel:
                del bot.active_sessions[key]
                logger.info(f"Removed ghost session for {user_id} in guild {guild_id}.")
                continue

        # Check members currently in voice
        for guild in bot.guilds:
            for voice_channel in guild.voice_channels:
                for member in voice_channel.members:
                    if member.bot:
                        continue
                        
                    audio_key = (member.id, guild.id)
                    
                    if audio_key in bot.active_sessions:
                        continue
                        
                    max_estimated_duration = timedelta(minutes=5)
                    estimated_start = datetime.now(pytz.UTC) - max_estimated_duration
                    
                    try:
                        last_join = await bot.db.get_user_activity(member.id, guild.id)
                        if last_join and last_join.get('last_voice_join'):
                            estimated_start = min(
                                estimated_start,
                                last_join['last_voice_join']
                            )
                    except Exception as e:
                        logger.error(f"Error getting last join for {member}: {e}")
                    
                    bot.active_sessions[audio_key] = {
                        'start_time': estimated_start,
                        'last_audio_time': datetime.now(pytz.UTC),
                        'audio_disabled': member.voice.self_deaf or member.voice.deaf,
                        'total_audio_off_time': 0,
                        'estimated': True,
                        'max_estimated_time': datetime.now(pytz.UTC) + max_estimated_duration
                    }
                    
                    logger.info(f"Estimated session created for {member.display_name} in channel {voice_channel.name} - Start: {estimated_start}")
                    
        logger.info("Voice channel member check completed")
        
    except Exception as e:
        logger.error(f"Error in voice channel check: {e}")

@log_task_metrics("detect_missing_voice_leaves")
async def detect_missing_voice_leaves():
    """Detects sessions that likely ended during downtime"""
    await bot.wait_until_ready()
    
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Database not initialized - skipping missing session detection")
        return
    
    try:
        logger.info("Starting detection of lost voice sessions...")
        
        async with bot.db.pool.acquire() as conn:
            active_sessions = await conn.fetch('''
                SELECT user_id, guild_id, last_voice_join 
                FROM user_activity 
                WHERE (last_voice_leave IS NULL OR last_voice_leave < last_voice_join)
                AND last_voice_join < NOW() - INTERVAL '10 minutes'
            ''')
            
        for session in active_sessions:
            guild = bot.get_guild(session['guild_id'])
            if not guild:
                continue
                
            member = guild.get_member(session['user_id'])
            if not member:
                continue
                
            if not member.voice or not member.voice.channel:
                join_time = session['last_voice_join']
                if join_time.tzinfo is None:
                    join_time = join_time.replace(tzinfo=pytz.UTC)
                
                max_duration = timedelta(hours=24).total_seconds()
                duration = min((datetime.now(pytz.UTC) - join_time).total_seconds(), max_duration)
                
                try:
                    await bot.db.log_voice_leave(member.id, guild.id, int(duration))
                    
                    audio_key = (member.id, guild.id)
                    if audio_key in bot.active_sessions:
                        del bot.active_sessions[audio_key]
                        
                    logger.info(f"Session that ended during bot downtime was closed for {member.display_name} with duration of {duration//60:.0f} minutes")
                    
                except Exception as e:
                    logger.error(f"Error logging estimated leave: {e}")
        
        logger.info("Lost voice session detection completed")
    except Exception as e:
        logger.error(f"Error in missing session detection: {e}")

@log_task_metrics("process_pending_voice_events")
async def process_pending_voice_events():
    """Processes pending voice events saved in the database"""
    await bot.wait_until_ready()
    
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Database not initialized - skipping pending event processing")
        return
    
    try:
        pending_events = await bot.db.get_pending_voice_events(limit=500)
        if not pending_events:
            logger.info("No pending voice events to process")
            return
            
        logger.info(f"Processing {len(pending_events)} pending voice events...")
        
        batch_size = 50
        processed_ids = []
        
        for event in pending_events:
            try:
                guild = bot.get_guild(event['guild_id'])
                if not guild:
                    processed_ids.append(event['id'])
                    continue
                    
                member = guild.get_member(event['user_id'])
                if not member:
                    processed_ids.append(event['id'])
                    continue
                
                before_channel = guild.get_channel(event['before_channel_id']) if event['before_channel_id'] else None
                after_channel = guild.get_channel(event['after_channel_id']) if event['after_channel_id'] else None
                
                before_data = {
                    'channel_id': event['before_channel_id'],
                    'self_deaf': event['before_self_deaf'],
                    'deaf': event['before_deaf'],
                    'self_mute': False,
                    'mute': False,
                    'self_stream': False,
                    'self_video': False,
                    'suppress': False,
                    'requested_to_speak_at': None,
                }
                
                after_data = {
                    'channel_id': event['after_channel_id'],
                    'self_deaf': event['after_self_deaf'],
                    'deaf': event['after_deaf'],
                    'self_mute': False,
                    'mute': False,
                    'self_stream': False,
                    'self_video': False,
                    'suppress': False,
                    'requested_to_speak_at': None,
                }
                
                await bot.voice_event_queue.put((
                    event['event_type'],
                    member,
                    discord.VoiceState(data=before_data, channel=before_channel),
                    discord.VoiceState(data=after_data, channel=after_channel)
                ))
                
                processed_ids.append(event['id'])
                
            except Exception as e:
                logger.error(f"Error processing pending event {event['id']}: {e}")
                continue
                
            if len(processed_ids) >= batch_size:
                await bot.db.mark_events_as_processed(processed_ids)
                processed_ids = []
                await asyncio.sleep(1)
        
        if processed_ids:
            await bot.db.mark_events_as_processed(processed_ids)
        
        logger.info("Pending event processing completed")
        
    except Exception as e:
        logger.error(f"Error in pending event processing: {e}")

@log_task_metrics("cleanup_ghost_sessions")
async def cleanup_ghost_sessions():
    """Cleans up ghost sessions in the database"""
    await bot.wait_until_ready()
    
    if not hasattr(bot, 'db') or not bot.db or not bot.db._is_initialized:
        logger.error("Database not initialized - skipping ghost session cleanup")
        return
    
    try:
        logger.info("Starting ghost session cleanup...")
        
        now = datetime.now(pytz.UTC)
        to_remove = []
        
        for key, session in bot.active_sessions.items():
            if session.get('estimated') and 'max_estimated_time' in session:
                if now > session['max_estimated_time']:
                    to_remove.append(key)
                    try:
                        duration = (session['max_estimated_time'] - session['start_time']).total_seconds()
                        await bot.db.log_voice_leave(key[0], key[1], int(duration))
                    except Exception as e:
                        logger.error(f"Error logging estimated leave: {e}")
        
        for key in to_remove:
            bot.active_sessions.pop(key, None)
            logger.info(f"Removed expired estimated session for user {key[0]} in guild {key[1]}")
        
        async with bot.db.pool.acquire() as conn:
            ghost_sessions = await conn.fetch('''
                UPDATE user_activity 
                SET last_voice_leave = COALESCE(last_voice_join, NOW()) + INTERVAL '1 hour',
                    total_voice_time = total_voice_time + 3600
                WHERE (last_voice_leave IS NULL OR last_voice_join > last_voice_leave)
                AND (last_voice_join < NOW() - INTERVAL '24 hours')
                RETURNING user_id, guild_id, last_voice_join
            ''')
            
            if ghost_sessions:
                logger.info(f"Cleanup completed: {len(ghost_sessions)} ghost sessions corrected")
                
                for session in ghost_sessions:
                    try:
                        join_time = session['last_voice_join']
                        if join_time.tzinfo is None:
                            join_time = join_time.replace(tzinfo=pytz.UTC)
                            
                        await conn.execute('''
                            INSERT INTO voice_sessions
                            (user_id, guild_id, join_time, leave_time, duration)
                            VALUES ($1, $2, $3, $4, $5)
                        ''', 
                        session['user_id'], 
                        session['guild_id'],
                        join_time,
                        join_time + timedelta(hours=1),
                        3600)
                    except Exception as e:
                        logger.error(f"Error logging ghost session: {e}")
                        continue
    except Exception as e:
        logger.error(f"Error in ghost session cleanup: {e}")

async def cleanup_ghost_sessions_wrapper():
    """Wrapper for the ghost session cleanup task"""
    monitoring_period = 1
    task = bot.loop.create_task(execute_task_with_persistent_interval(
        "cleanup_ghost_sessions",
        cleanup_ghost_sessions
    ), name='cleanup_ghost_sessions_wrapper')
    return task

# ==============================================================================
# EVENT HANDLERS
# ==============================================================================

@bot.event
async def on_shutdown():
    """Executes cleanup actions before bot shutdown."""
    logger.info("Bot is shutting down - performing cleanup...")
    await bot.log_action("Shutdown", None, "Bot is shutting down")
    await emergency_backup()

@bot.event
async def on_resumed():
    """Executes actions when the bot reconnects after downtime."""
    logger.info("Bot reconnected after downtime")
    
    await check_current_voice_members()
    await detect_missing_voice_leaves()
    
    await bot.clear_queues()
    
    await bot.log_action("Reconnection", None, "Bot reconnected after downtime - Queues reinitialized")
    await process_pending_voice_events()

@bot.event
async def on_disconnect():
    """Executes actions when the bot is disconnected."""
    logger.warning("Bot disconnected - performing emergency backup...")

    try:
        active_voice_members = []
        for guild in bot.guilds:
            for member in guild.members:
                if member.voice and member.voice.channel:
                    active_voice_members.append(f"{member.display_name} in {member.voice.channel.name}")
        if active_voice_members:
            logger.info(f"Members in voice channels at disconnect: {', '.join(active_voice_members)}")
        else:
            logger.info("No members in voice channels at disconnect.")
    except Exception as e:
        logger.error(f"Error logging voice members at disconnect: {e}")

    await emergency_backup()

def handle_exception(loop, context):
    """Captures unhandled exceptions."""
    logger.error(f"Unhandled exception: {context['message']}", exc_info=context.get('exception'))
    asyncio.create_task(bot.log_action(
        "Unhandled Exception",
        None,
        f"Exception: {context.get('exception')}\nMessage: {context['message']}"
    ))

async def emergency_backup():
    """Performs emergency backup of critical data."""
    try:
        logger.info("Starting emergency backup...")
        try:
            sessions_backup_path = os.path.join('backups', 'active_sessions_backup.json')
            with open(sessions_backup_path, 'w', encoding='utf-8') as f:
                json.dump({str(k): v for k, v in bot.active_sessions.items()}, f, indent=4)
            logger.info(f"Active sessions backup saved to {sessions_backup_path}")
        except Exception as e:
            logger.error(f"Failed to save active sessions backup: {e}")

        if not hasattr(bot, 'db_backup') or bot.db_backup is None:
            try:
                if not hasattr(bot, 'db') or not bot.db:
                    logger.error("Database not available for backup")
                    return False
                    
                from database import DatabaseBackup
                bot.db_backup = DatabaseBackup(bot.db)
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error creating DatabaseBackup instance: {e}")
                return False
        
        try:
            if hasattr(bot.db_backup, 'create_backup'):
                success = await bot.db_backup.create_backup()
                
                if success:
                    logger.info("Emergency backup completed successfully.")
                    return True
                else:
                    logger.error("Emergency backup failed.")
                    return False
            else:
                logger.error("create_backup method not available in DatabaseBackup")
                return False
        except Exception as e:
            logger.error(f"Backup failure: {e}")
            return False
    except Exception as e:
        logger.error(f"Unexpected error during emergency backup: {e}")
        return False

# ==============================================================================
# TASK INITIALIZATION
# ==============================================================================

async def initialize_tasks():
    """Initializes all background tasks."""
    await start_unified_inactivity_manager()
    await start_cleanup_members()
    await start_database_backup()
    await start_cleanup_old_data()
    await cleanup_ghost_sessions_wrapper()
    
    # Voice-related tasks
    bot.loop.create_task(voice_event_processor(), name='voice_event_processor')
    bot.loop.create_task(check_current_voice_members(), name='check_current_voice_members')
    bot.loop.create_task(detect_missing_voice_leaves(), name='detect_missing_voice_leaves')
    bot.loop.create_task(process_pending_voice_events(), name='process_pending_voice_events')
    
    # Configure exception handler
    bot.loop.set_exception_handler(handle_exception)