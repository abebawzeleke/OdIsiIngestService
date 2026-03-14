-- ============================================================
-- OdIsiLiveVector cleanup script
-- Run ONCE before deploying the updated Worker.cs
--
-- This script:
--   1. Keeps only the latest row per channel
--   2. Sets LiveKey = Channel on the surviving rows
--   3. Deletes all other rows (~159K stale inserts)
--   4. Adds a unique index so only 1 row per channel can exist
-- ============================================================

BEGIN TRAN;

-- Step 1: Identify the latest row per channel
;WITH ranked AS (
    SELECT Id,
           Channel,
           ROW_NUMBER() OVER (PARTITION BY Channel ORDER BY TimestampUtc DESC) AS rn
    FROM dbo.OdIsiLiveVector
)
DELETE FROM ranked WHERE rn > 1;

PRINT 'Deleted stale rows. Remaining row count:';
SELECT Channel, COUNT(*) AS RowCount FROM dbo.OdIsiLiveVector GROUP BY Channel;

-- Step 2: Set LiveKey = Channel on surviving rows
UPDATE dbo.OdIsiLiveVector
SET LiveKey = Channel
WHERE LiveKey IS NULL OR LiveKey <> Channel;

PRINT 'Set LiveKey = Channel on all surviving rows.';
SELECT Id, LiveKey, Channel, TimestampUtc, GaugeCount FROM dbo.OdIsiLiveVector ORDER BY Channel;

-- Step 3: Add unique index to prevent future unbounded growth
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UX_OdIsiLiveVector_LiveKey_Channel'
      AND object_id = OBJECT_ID('dbo.OdIsiLiveVector')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_OdIsiLiveVector_LiveKey_Channel
        ON dbo.OdIsiLiveVector (LiveKey, Channel);
    PRINT 'Created unique index UX_OdIsiLiveVector_LiveKey_Channel.';
END
ELSE
    PRINT 'Unique index already exists.';

COMMIT;

PRINT 'Cleanup complete. OdIsiLiveVector should now have exactly 4 rows.';
