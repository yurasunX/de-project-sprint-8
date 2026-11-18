CREATE TABLE IF NOT EXISTS public.subscribers_feedback (
    id serial4 PRIMARY KEY,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created int4 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT subscribers_feedback_event UNIQUE (restaurant_id, adv_campaign_id, client_id)
);
-- Also prepare an existing compatible table created from the lesson DDL.
-- Existing duplicate event keys must be resolved before this migration.
CREATE UNIQUE INDEX IF NOT EXISTS subscribers_feedback_event
    ON public.subscribers_feedback (restaurant_id, adv_campaign_id, client_id);
