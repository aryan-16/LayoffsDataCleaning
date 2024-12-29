SELECT * 
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company , location , industry , total_laid_off , percentage_laid_off , `date` , country , stage , funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company , location , industry , total_laid_off , percentage_laid_off , `date` , stage , funds_raised_millions, country ) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company , 
location , industry , total_laid_off , 
percentage_laid_off , `date` , country 
, stage , funds_raised_millions) AS row_num
FROM layoffs_staging;



DELETE
FROM layoffs_staging2
WHERE row_num>1;

-- STANDARDIZATION

UPDATE layoffs_staging2
SET company = TRIM(company);

select distinct industry 
from layoffs_staging2
ORDER by 1;

select *
from layoffs_staging2
WHERE industry like 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry like 'Crypto%';

SELECT distinct location
FROM layoffs_staging2
order by 1;

select `date`
from layoffs_staging2;

select `date`,
str_to_date(`date` , '%m/%d/%Y')
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date` , '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` DATE;


-- BLANK AND NULL

select *
from layoffs_staging2
where industry is null
OR industry = '';

SELECT *
FROM layoffs_staging2
where company = 'Airbnb';

update layoffs_staging2
set industry = null
where industry = '';

select * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2 
   on t1.company = t2.company
   where t1.industry is null 
   AND t2.industry is not null ;
 
 update  layoffs_staging2 t1 
         join layoffs_staging2 t2 
		 ON t1.company = t2.company
SET t1.industry = t2.industry 
   where t1.industry is null 
   and t2.industry is not null;
 
-- deleting waste rows 

select * 
 from layoffs_staging2 
 where total_laid_off is null 
 and percentage_laid_off is null;
 
 
delete
 from layoffs_staging2 
 where total_laid_off is null 
 and percentage_laid_off is null;


-- deleting redundant column 

alter table layoffs_staging2
drop column row_num;

