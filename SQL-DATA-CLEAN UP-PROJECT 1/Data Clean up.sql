-- SQL Project - Data Cleaning


-- Check data in the original layoffs table
SELECT * 
FROM world_layoffs.layoffs;

-- Create a staging table to clean and manipulate the data
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

-- Insert data from the original table into the staging table
INSERT INTO world_layoffs.layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- Data cleaning steps:

-- 1. Check for duplicates and remove them
-- First, let's check for duplicates
SELECT company, industry, total_laid_off, `date`,
    ROW_NUMBER() OVER (
        PARTITION BY company, industry, total_laid_off, `date`
    ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Now let's identify duplicates based on row number greater than 1
SELECT * 
FROM (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
        ROW_NUMBER() OVER (
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS row_num
    FROM world_layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;

-- Check for duplicates in specific companies like 'Oda' to confirm
SELECT * 
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda';

-- Clean duplicates by removing those with row_num > 1
WITH DELETE_CTE AS (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
        ROW_NUMBER() OVER (
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS row_num
    FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
    FROM DELETE_CTE
) AND row_num > 1;

-- Create a new column to store row numbers and delete duplicates
ALTER TABLE world_layoffs.layoffs_staging ADD COLUMN row_num INT;

-- Insert row numbers into the staging table
UPDATE world_layoffs.layoffs_staging
SET row_num = (
    SELECT ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) FROM world_layoffs.layoffs_staging
);

-- Now delete rows where row_num is greater than 1 (duplicates)
DELETE FROM world_layoffs.layoffs_staging
WHERE row_num > 1;

-- Create a new table 'layoffs_staging2' for cleaned data
CREATE TABLE world_layoffs.layoffs_staging2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT,
    percentage_laid_off TEXT,
    `date` DATE,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT,
    row_num INT
);

-- Insert cleaned data into 'layoffs_staging2'
INSERT INTO world_layoffs.layoffs_staging2
SELECT 
    company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Delete rows where row_num is greater than 1 in the new table
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

-- 2. Standardize Data

-- Check the distinct values for 'industry'
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- Update empty strings in the 'industry' column to NULL
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Look for rows where industry is NULL or empty
SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
ORDER BY industry;

-- Populate NULL 'industry' values from other rows with the same company name
UPDATE world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2 ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Check if there are any remaining NULL 'industry' values
SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL
ORDER BY industry;

-- Standardize variations of 'Crypto' in the 'industry' column
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Verify that the industry column is standardized
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- Standardize 'United States' country name (remove trailing period)
UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Check if the country column has been standardized
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

-- Standardize date format by converting it to DATE type
UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Change the column type to DATE
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Check the updated table
SELECT * 
FROM world_layoffs.layoffs_staging2;



-- 4. Remove unnecessary columns or rows

-- Check for rows with NULL values in total_laid_off and percentage_laid_off
SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Remove rows with both total_laid_off and percentage_laid_off as NULL
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Drop the 'row_num' column as it's no longer needed
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

-- Final verification of the cleaned data
SELECT * 
FROM world_layoffs.layoffs_staging2;
