SELECT * FROM covid_deaths
WHERE continent IS NULL

/* Table for Covid_Deaths */
CREATE TABLE covid_deaths
(iso_code VARCHAR(10), continent VARCHAR(25), location VARCHAR(25), date DATE,
 total_cases INT, population INT, new_cases INT, new_cases_smoothed DECIMAL,
 total_deaths INT, new_deaths INT, new_deaths_smoothed DECIMAL, total_cases_per_million DECIMAL,
 new_cases_per_million DECIMAL, new_cases_smoothed_per_million DECIMAL,
 total_deaths_per_million DECIMAL, new_deaths_per_million DECIMAL,
 new_deaths_smoothed_per_million DECIMAL,reproduction_rate DECIMAL,
 icu_patients INT, icu_patients_per_million DECIMAL, hosp_patients INT,
 hosp_patients_per_million DECIMAL, weekly_icu_admissions DECIMAL,
 weekly_icu_admissions_per_million DECIMAL, weekly_hosp_admissions DECIMAL,
 weekly_hosp_admissions_per_million DECIMAL
)

ALTER TABLE covid_deaths
ALTER COLUMN population TYPE BIGINT;

ALTER TABLE covid_deaths
ALTER COLUMN location TYPE VARCHAR(50)

COPY covid_deaths
FROM 'D:\Tutorials\Alex the Analyst\CovidDeaths.csv'
DELIMITER ','
CSV HEADER;

/* Table for Covid_Vaccinations */
CREATE TABLE covid_vaccinations
(iso_code VARCHAR(10), continent VARCHAR(25), location VARCHAR(50), date DATE,
 new_tests INT, total_tests INT, total_tests_per_thousands DECIMAL, new_tests_per_thousands DECIMAL,
 new_tests_smoothed DECIMAL, new_tests_smoothed_per_thousands DECIMAL, positive_rate DECIMAL,
 tests_per_case DECIMAL, tests_units TEXT, total_vaccinations BIGINT, people_vaccinated BIGINT,
 people_fully_vaccinated BIGINT, new_vaccinations BIGINT, new_vaccinations_smoothed BIGINT,
 total_vaccinations_per_hundred DECIMAL, people_vaccinated_per_hundred DECIMAL,
 people_fully_vaccinated_per_hundred DECIMAL, new_vaccinations_smoothed_per_million DECIMAL,
 stringency_index DECIMAL, population_density DECIMAL, median_age DECIMAL, aged_65_older DECIMAL,
 aged_70_older DECIMAL, gdp_per_capita DECIMAL, extreme_poverty DECIMAL, cardiovasc_death_rate DECIMAL,
 diabetes_prevalence DECIMAL, female_smokers DECIMAL, male_smokers DECIMAL,
 handwashing_facilities DECIMAL, hospital_beds_per_thousand DECIMAL, life_expectancy DECIMAL,
 human_development_index DECIMAL
)

COPY covid_vaccinations
FROM 'D:\Tutorials\Alex the Analyst\CovidVaccinations.csv'
DELIMITER ','
CSV HEADER;

--Looking at Total Cases per Total Deaths
--Shows likelihood of dying if you contract COVID in your country

CREATE VIEW  TotalCovidCases_vs_TotalCovidDeaths AS
SELECT location, date, total_deaths, total_cases,
ROUND(((CAST((total_deaths) AS DECIMAL))/(total_cases))*100,2) AS death_rate FROM covid_deaths
WHERE location LIKE '%Tanzania%'
ORDER BY date ASC

--Looking at covid cases per Population
--Shows countries COVID infection percentage

CREATE OR REPLACE VIEW Daily_CovidInfection_Rate_SA AS
SELECT location, date, total_cases, population,
ROUND(((CAST((total_cases) AS DECIMAL))/population)*100,8) AS infection_rate FROM covid_deaths
WHERE location LIKE '%outh Af%' AND total_cases IS NOT NULL
ORDER BY infection_rate DESC


--Looking at total covid cases per Population
--Shows countries with highest COVID infection percentage

CREATE OR REPLACE VIEW CovidInfectionRates_Per_Country AS
SELECT location, MAX(total_cases), population,
ROUND(((CAST(MAX(total_cases) AS DECIMAL))/population)*100,6) AS infection_rate FROM covid_deaths
WHERE location NOT LIKE '%International%' AND  location NOT LIKE '%World%' AND total_cases IS NOT NULL
GROUP BY location,population
ORDER BY infection_rate DESC

--Looking at total covid deaths in a country
--Showing Countries total death toll due to COVID

CREATE VIEW Covid_Death_Toll_Per_Country AS
SELECT location, MAX(total_deaths) AS TotalDeathCount FROM covid_deaths
WHERE continent IS NOT NULL AND total_deaths IS NOT NULL
GROUP BY location
ORDER BY TotalDeathCount DESC

--Looking at total covid deaths per Population of a country
--Showing Countries percentage total death toll due to COVID in a given population

CREATE VIEW CovidDeathRate_Per_Country AS
SELECT location, MAX(total_deaths) AS TotalDeathCount, population,
ROUND((CAST(MAX(total_deaths) AS NUMERIC)/population * 100),6) AS death_rate FROM covid_deaths
WHERE continent IS NOT NULL AND total_deaths IS NOT NULL
GROUP BY location, population
ORDER BY TotalDeathCount DESC

--Let's look at the situation globally

--Looking at the total deaths due to covid in each continent

CREATE VIEW CovidDeathToll_Per_Continent AS
SELECT location, MAX(total_deaths) AS TotalDeathCount FROM covid_deaths
WHERE continent IS NULL
GROUP BY location
ORDER BY TotalDeathCount DESC

-- Looking at the total covid cases and total covid deaths in each continent
CREATE VIEW CovidDeathCountvsCovidCases_Globally AS
SELECT continent, SUM(new_deaths) AS TotalDeathsCount, SUM(new_cases) AS TotalCasesCount FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY TotalDeathsCount DESC

-- Looking at the total covid cases, total deaths due to covid and %of deaths per cases in the globe each day
CREATE VIEW CovidDeaths_Per_CovidCases_Globally AS
SELECT date, SUM(new_deaths) AS TotalDeathsCount, SUM(new_cases) AS TotalCasesCount,
SUM(CAST((new_deaths) AS DECIMAL))/SUM(new_cases)*100 AS DeathRate
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY date ASC

-- Looking at the total covid cases, total covid deaths & %of deaths per cases in the globe

CREATE VIEW TotalCovidDeaths_vs_TotalCovidCases_Globally AS
SELECT SUM(new_deaths) AS TotalDeathsCount, SUM(new_cases) AS TotalCasesCount,
SUM(CAST((new_deaths) AS DECIMAL))/SUM(new_cases)*100 AS DeathRate
FROM covid_deaths
WHERE continent IS NOT NULL

SELECT * FROM covid_vaccinations

-- JOINING covid_deaths & covid_vaccinations tables


SELECT * FROM covid_deaths AS cd
JOIN covid_vaccinations AS cv
ON cd.location = cv.location AND cd.date = cv.date

-- Population Vaccinated per Country each day
CREATE VIEW Daily_Vaccinations_Per_Country AS
SELECT cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations FROM covid_deaths AS cd
JOIN covid_vaccinations AS cv
ON cd.location = cv.location AND cd.date = cv.date
WHERE cd.continent IS NOT NULL
ORDER BY cd.location, cd.continent, cd.date

-- Population Vaccinated per Country
CREATE VIEW Rolling_People_Vaccinated_Per_Country AS
SELECT cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations,
SUM(cv.new_vaccinations) OVER (PARTITION BY cd.location ORDER BY (cd.location,cd.date))
AS rolling_people_vaccinated
FROM covid_deaths AS cd
JOIN covid_vaccinations AS cv
ON cd.location = cv.location AND cd.date = cv.date
WHERE cd.continent IS NOT NULL
ORDER BY cd.location, cd.continent, cd.date

-- Rolling Percentage of Population Vaccinated per Country
CREATE VIEW Rolling_Percentage_Vaccinated_Per_Country AS
WITH PopVacc
AS (SELECT cd.continent, cd.location, cd.date, cd.population,
cv.new_vaccinations,
SUM(cv.new_vaccinations) OVER (PARTITION BY cd.location ORDER BY (cd.location,cd.date))
AS rolling_people_vaccinated
FROM covid_deaths AS cd
JOIN covid_vaccinations AS cv
ON cd.location = cv.location AND cd.date = cv.date
WHERE cd.continent IS NOT NULL
ORDER BY cd.location, cd.continent, cd.date)

SELECT *,
(rolling_people_vaccinated/population)*100
FROM PopVacc


--CREATE TABLE
DROP TABLE IF EXISTS PercentPopulationVaccinated
CREATE TABLE PercentPopulationVaccinated
(continent varchar(255),
 location varchar(255),
 date DATE,
 population bigint,
 new_vaccinations bigint,
 rolling_people_vaccinated numeric
)

INSERT INTO PercentPopulationVaccinated
SELECT cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations,
SUM(cv.new_vaccinations) OVER (PARTITION BY cd.location ORDER BY (cd.location,cd.date))
AS rolling_people_vaccinated
FROM covid_deaths AS cd
JOIN covid_vaccinations AS cv
ON cd.location = cv.location AND cd.date = cv.date
WHERE cd.continent IS NOT NULL
ORDER BY cd.location, cd.continent, cd.date

SELECT *,
(rolling_people_vaccinated/population)*100
FROM PercentPopulationVaccinated