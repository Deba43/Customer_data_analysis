-- Create a new table to store cleaned data
CREATE TABLE customer_clean AS

-- Step 1️⃣: Create a base dataset with row numbers per 'name' to identify duplicates
WITH base AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY name ORDER BY id) AS rn
  FROM customer
),

-- Step 2️⃣: Filter out unwanted or invalid rows
filtered AS (
  SELECT *
  FROM base
  WHERE rn = 1                                  -- Keep only the first record per name (deduplication)
    AND id IS NOT NULL                          -- Exclude rows missing IDs
    AND gender IS NOT NULL                      -- Ensure gender is present
    AND trim(CAST(gender AS varchar)) <> ''     -- Remove empty strings masquerading as valid data
    AND TRY_CAST(age AS integer) IS NOT NULL    -- Age must be a valid integer
    AND TRY_CAST(age AS integer) BETWEEN 18 AND 80  -- Keep realistic age range
    -- Validate that annual income is numeric and not zero
    AND TRY_CAST(regexp_replace(CAST(annual_income AS varchar), '[^0-9.]', '') AS double) IS NOT NULL
    AND TRY_CAST(regexp_replace(CAST(annual_income AS varchar), '[^0-9.]', '') AS double) <> 0
),

-- Step 3️⃣: Data cleaning and normalization
cleaned AS (
  SELECT
    id,

    -- 🧹 Clean 'name' by removing all special characters and digits
    regexp_replace(
      regexp_replace(CAST(name AS varchar), '[^A-Za-z\\s]', ''), -- Keep only alphabets and spaces
      '[0-9]', ''                                               -- Remove any digits that slipped in
    ) AS name_clean,

    -- 🧩 Normalize 'gender' to uppercase (e.g., 'male' → 'MALE')
    CASE
      WHEN gender IS NULL OR trim(CAST(gender AS varchar)) = '' THEN NULL
      ELSE upper(trim(CAST(gender AS varchar)))
    END AS gender,

    -- Convert text fields to numeric types for analysis
    TRY_CAST(age AS integer) AS age,
    marital_status,
    education,

    -- 🕒 Extract numeric digits from employment_years (e.g., "10 yrs" → 10)
    TRY_CAST(regexp_replace(CAST(employment_years AS varchar), '[^0-9]', '') AS integer) AS employment_years,

    -- 💰 Clean income and credit fields to remove currency symbols, commas, etc.
    TRY_CAST(regexp_replace(CAST(annual_income AS varchar), '[^0-9.]', '') AS double) AS annual_income,
    TRY_CAST(regexp_replace(CAST(credit_limit AS varchar), '[^0-9.]', '') AS double) AS credit_limit,
    TRY_CAST(regexp_replace(CAST(current_balance AS varchar), '[^0-9.]', '') AS double) AS current_balance,

    -- 0/1 flag: did the customer default next month?
    TRY_CAST(default_next_month AS integer) AS default_next_month
  FROM filtered
),

-- Step 4️⃣: Add derived metrics or computed columns
final AS (
  SELECT *,
         -- 📊 Compute utilization ratio = current_balance / credit_limit
         CASE
           WHEN credit_limit IS NULL OR credit_limit = 0 THEN NULL
           ELSE current_balance / credit_limit
         END AS utilization
  FROM cleaned
)

-- Step 5️⃣: Return final cleaned dataset ordered by name
SELECT *
FROM final
ORDER BY name_clean ASC NULLS FIRST;
