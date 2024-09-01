'''
@Author: Girish
@Date: 2024-08-23
@Last Modified by: Girish
@Last Modified: 2024-08-23
@Title: Covid daanalysis Problems.
'''


use Covid_test;

--Sql operation on Covid-data-global

--1.To find out the death percentage locally and globally


-- Locally
select country_region, round(deaths*100.00/confirmed,2) as death_percentage from country_wiselatest ;

-- Globally
select country_region, round(deaths*100.00/confirmed,2)/1 as death_percentage from country_wiselatest  where country_region='india';



--2. To find out the infected population percentage locally and globally

-- Locally
select Country_Region,round((totalcases *100.0 /population),4)  as infectedPopulationPercentage from worldometer_data; 

---Globally
select Country_Region,round((totalcases *100.0 /population),4)  as infectedPopulationPercentage from worldometer_data where Country_Region='india';



--3. To find out the countries with the highest infection rates

select Country_Region,Confirmed as highest_infection from country_wiselatest where Confirmed in (select max(Confirmed) from country_wiselatest );



--4. To find out the countries and continents with the highest death counts


--Continent
select top 1 Continent,sum(totaldeaths) as totaldeath from worldometer_data group by Continent  order by TotalDeath desc ;


--Country
select top 1 Country_region, totaldeaths from worldometer_data order by TotalDeaths desc;





---5. Average number of deaths by day (Continents and Countries)


--Country
select Country_Region,sum(deaths)/count(Country_Region) as average_deaths_countries from covid_clean group by Country_Region;  

--Continent
select who_region,sum(deaths)/count(who_region) average_death_continent from covid_clean group by WHO_Region; 




---6. Average of cases divided by the number of population of each country (TOP 10) 


select top 10 Country_Region,(TotalCases * 1.0 / Population) AS Cases_Per_Population FROM worldometer_data order by Cases_Per_Population desc;




---7. Considering the highest value of total cases, which countries have the highest rate of infection in relation to population

select top 1 country_region,totaldeaths,totalcases*1.0/population as infection_rate from worldometer_data order by totaldeaths desc;