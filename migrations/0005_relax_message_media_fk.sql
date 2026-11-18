-- Version 5: drop the message_media -> messages FK.
--
-- Rationale: async media download workers can lose the race with message
-- deletion/moves (we do not hard-delete messages in prod, but sweep retries
-- after a chat is removed from targets would attempt to attach orphan files
-- to no-longer-existing parent rows, producing FK violations that appear as
-- noise but block legitimate downloads from being recorded). The M2M
-- relationship is still enforced at the application layer: attach_media_to_message
-- only runs after upsert_message succeeds.
BEGIN;
ALTER TABLE message_media DROP CONSTRAINT IF EXISTS fk_mm_chat_msg;
COMMIT;
