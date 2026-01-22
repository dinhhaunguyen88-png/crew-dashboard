-- =====================================================
-- CREATE STANDBY_RECORDS TABLE
-- Run this SQL in Supabase SQL Editor:
-- Dashboard -> SQL Editor -> New Query -> Paste & Run
-- =====================================================

-- Drop if exists (for clean reinstall)
DROP TABLE IF EXISTS standby_records;

-- Create the table
CREATE TABLE standby_records (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    crew_id TEXT NOT NULL,
    crew_name TEXT,
    base TEXT,
    ac_type TEXT,
    position TEXT,
    duty_type TEXT NOT NULL,  -- SBY, OSBY, SL, CSL
    duty_date TEXT NOT NULL,  -- Date format: DD/MM/YY
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(crew_id, duty_type, duty_date)
);

-- Create indexes for fast filtering
CREATE INDEX idx_standby_crew ON standby_records(crew_id);
CREATE INDEX idx_standby_date ON standby_records(duty_date);
CREATE INDEX idx_standby_type ON standby_records(duty_type);

-- Enable Row Level Security
ALTER TABLE standby_records ENABLE ROW LEVEL SECURITY;

-- Allow anonymous access (for demo)
DROP POLICY IF EXISTS "Allow all access to standby_records" ON standby_records;
CREATE POLICY "Allow all access to standby_records" ON standby_records FOR ALL USING (true) WITH CHECK (true);

-- Verify table created
SELECT 'standby_records table created successfully!' as status;
