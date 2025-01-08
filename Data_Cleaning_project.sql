-- Data Cleaning
-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null values or Blank values
-- 4. Remove Any Columns or Rows

------------------------------------------------------------------------------------------------------------------------------

-- 1. Remove Duplicates

create table Layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert into layoffs_staging
select *
from Layoffs;

select *
from layoffs_staging;


select *,
Row_Number() over(
partition by company, industry, total_laid_off,percentage_laid_off, `date`) as row_num
from layoffs_staging;


with duplicate_cte as
(
select *,
Row_Number() over(
partition by company, location, industry, total_laid_off,percentage_laid_off, `date`, stage, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;


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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


insert into layoffs_staging2
select *,
Row_Number()
over(
partition by company, location, industry, total_laid_off,percentage_laid_off, `date`, stage, funds_raised_millions) as row_num
from layoffs_staging
;

select *
from layoffs_staging2
where row_num >1;

delete 
from layoffs_staging2
where row_num > 1
;

select *
from layoffs_staging2
;

-- 2.Standardizing

select company, trim(company)
from layoffs_staging2;


update layoffs_staging2
set company = trim(company);


select *
from layoffs_staging2
where industry like 'crypto%'
order by 1;

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

select distinct ( industry)
from layoffs_staging2;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` date;

select `date`
from layoffs_staging2;

-- 3.Dealing Null values or Blank values

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = '';


select *
from  layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update  layoffs_staging2
set industry = null
where industry = '';

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;
    
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


-- 4. Remove Any unwanted Columns or Rows 

alter table layoffs_staging2
drop column row_num;

    
select *
from layoffs_staging2
