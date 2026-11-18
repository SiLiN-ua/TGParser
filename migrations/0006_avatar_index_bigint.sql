-- Version 6: fix avatar_history unique index type inference.
--
-- `COALESCE(photo_file_id, 0)` typed the index expression as INT4 because
-- the literal `0` is INT4; real Telegram photo IDs are int64 and blow the
-- range. Recreate the index with an explicit bigint literal so the
-- expression type stays BIGINT, and the ON CONFLICT clause can use the
-- same expression without parameter type inference going sideways.
BEGIN;
DROP INDEX IF EXISTS uq_avatar_hist;
CREATE UNIQUE INDEX uq_avatar_hist
    ON avatar_history(user_id, COALESCE(photo_file_id, 0::bigint));
COMMIT;
