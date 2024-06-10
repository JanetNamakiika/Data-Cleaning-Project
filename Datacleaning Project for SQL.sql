SELECT * 
FROM layoffs;

##1. Remove Duplicates
##2. Standardize the data
##3. Look ar Null values
##4. Remove unecessary columns


CREATE TABLE Layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

SELECT * 
FROM layoffs_staging;

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry,percentage_laid_off, total_laid_off, `date`, stage, country, funds_raised_millions ) AS Row_num
FROM layoffs_staging;

WITH duplicate_cte AS 
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry,percentage_laid_off, total_laid_off, `date`, stage, country, funds_raised_millions) AS Row_num
FROM layoffs_staging)

SELECT*
FROM duplicate_cte
WHERE Row_num >1;

WITH duplicate_cte AS 
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry,percentage_laid_off, total_laid_off, `date`, stage, country, funds_raised_millions) AS Row_num
FROM layoffs_staging)

SELECT*
FROM duplicate_cte
WHERE Row_num >1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `Row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT*
FROM layoffs_staging2;

INSERT INTO layoffs_staging2 
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry,percentage_laid_off, total_laid_off, `date`, stage, country, funds_raised_millions) AS Row_num
FROM layoffs_staging);

SELECT*
FROM layoffs_staging2;

SELECT*
FROM layoffs_staging2
WHERE Row_num>1;

DELETE
FROM layoffs_staging2
WHERE Row_num>1;

SELECT*
FROM layoffs_staging2
WHERE Row_num>1;

##Standardizing Data
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company=TRIM(company);

SELECT company
FROM layoffs_staging2;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry='Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United states%'
ORDER BY country;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY country;


UPDATE layoffs_staging2
SET country= TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United states%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

SELECT `date`
FROM layoffs_staging2;

SELECT `date`
FROM layoffs_staging;


SELECT date,
str_to_date(date, '%d/%m/%Y')
FROM layoffs_staging;

UPDATE layoffs_staging2
SET `date`= STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT*
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT st2.industry, st3.industry
FROM layoffs_staging2 AS st2
JOIN layoffs_staging2 AS st3
ON st2.company=st3.company
WHERE (st2.industry IS NULL OR st2.industry='')
AND st3.industry IS NOT NULL;

SELECT*
FROM layoffs_staging2
WHERE company= 'Airbnb';


UPDATE layoffs_staging2 AS st2
JOIN layoffs_staging2 AS st3
ON st2.company=st3.company
SET st2.industry=st3.industry
WHERE st2.industry IS NULL 
AND st3.industry IS NOT NULL;

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN Row_num;
