CREATE OR REPLACE PROCEDURE connect.insert_new_f_calls()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO connect.f_calls (
        contact_id, init_contact_id, prev_contact_id, next_contact_id,
        channel, init_method, init_time, disconn_time, disconn_reason,
        last_update_time, agent_conn, agent_id, agent_username,
        agent_conn_att, agent_afw_start, agent_afw_end, agent_afw_duration,
        agent_interact_duration, agent_holds, agent_longest_hold,
        queue_id, queue_name, in_queue_time, out_queue_time, queue_duration,
        customer_phone, customer_voice, customer_hold_duration,
        sys_phone, conn_to_sys, contact_duration
    )
    SELECT
        s.contact_id, s.init_contact_id, s.prev_contact_id, s.next_contact_id,
        s.channel, s.init_method, s.init_time, s.disconn_time, s.disconn_reason,
        s.last_update_time, s.agent_conn, s.agent_id, s.agent_username,
        s.agent_conn_att, s.agent_afw_start, s.agent_afw_end, s.agent_afw_duration,
        s.agent_interact_duration, s.agent_holds, s.agent_longest_hold,
        s.queue_id, s.queue_name, s.in_queue_time, s.out_queue_time, s.queue_duration,
        s.customer_phone, s.customer_voice, s.customer_hold_duration,
        s.sys_phone, s.conn_to_sys, s.contact_duration
    FROM connect.f_calls_staging s
    LEFT JOIN connect.f_calls f ON s.contact_id = f.contact_id
    WHERE f.contact_id IS NULL;

    TRUNCATE TABLE connect.f_calls_staging;
    RAISE NOTICE 'insert_new_f_calls completed successfully.';
END;
$$;
