# NBA Data Analysis (1982–2022) — Academic Project using Databricks

## 1. Project Objective

### 1.1 Context 
The Most Valuable Player (MVP) award is given annually to the player considered to have delivered the most outstanding performance in the National Basketball Association (NBA) during the regular season. It is one of the most prestigious individual honours in professional sports and reflects a combination of statistical production, leadership, team impact and broader contextual factors.  

Studying MVP winners, as well as players who produced at a similar level without receiving the award, helps reveal how elite performance is recognised, how player roles evolve over time and how individual excellence does or does not align with team success.  

The primary objective of this project is to analyse NBA player and team data in order to answer a set of research questions related to MVP performance, positional tendencies, team success and standout seasons by non MVP players.

The sections below provide an overview of the project, details its methodology, and outline the analytical findings. It summarises the data sources, processing approach, modelling decisions and key results obtained from the analysis.  

The complete implementation, including all transformation steps, validation procedures and query outputs, is available in the notebook `NBA_MVP_Pipeline.ipynb` included in this repository.

### 1.2 Research Questions

The analysis is guided by five central questions:

1. Which statistics are most consistent among MVP winners across seasons?  
2. What is the distribution of positions among MVP winners?  
3. Do some teams historically concentrate more MVP awards?  
4. How often does the season MVP play for the team that wins the NBA championship?  
5. Which players delivered statistically outstanding seasons without winning the MVP award?

These questions explore patterns of individual performance, positional roles, organisational dominance and the relationship between elite regular season performance and postseason outcomes.

---
## 2. Data Sources

This project uses one primary dataset and three supplementary datasets.  
Together, they provide player statistics, MVP outcomes, team abbreviations and championship results required for the analytical steps of the work.  
Copies of the datasets are saved in the `CSV files` folder for reference.

### 2.1 Primary Dataset
The main dataset (NBA_Dataset.csv) was obtained from Kaggle:

**NBA Player Season Statistics With MVP and Win Share (1982 to 2022)**  
https://www.kaggle.com/datasets/robertsunderhaft/nba-player-season-statistics-with-mvp-win-share/data

It contains detailed regular season statistics for NBA players, including efficiency measures and MVP vote shares.

### 2.2 Supplementary Datasets
Three additional datasets were created manually by the author using public information from Basketball Reference website: https://www.basketball-reference.com/

These datasets are:
1. ListOfMVPs_AllSeasons.csv - Contains the list of MVP winners for each season and the team they represented.
2. Champions.csv - Records the NBA champion for each season and their conference.
3. Franchise_Abbrev.csv - Provides a mapping between NBA franchises and their standard abbreviations.

These supplementary files support data validation, table joins and the creation of gold layer analytical tables.

### 2.3 Data Ingestion
For practicality, all datasets used in this project were manually uploaded to Databricks in CSV format.  
Since all files were already available in structured tabular form, manual upload was chosen as a simple and efficient ingestion method for this project.  

---
## 3. Platform Used

The project was developed entirely in **Databricks Free Edition**. The platform was used for all core activities, including:

- managing the workspace and file system  
- loading the datasets into a Databricks Volume  
- creating bronze, silver and gold tables  
- performing data quality assessments  
- running SQL and PySpark transformations  
- preparing the analytical queries used to answer the research questions  

Databricks was selected because it provides an integrated environment that combines scalable compute, managed storage, SQL capabilities and notebook based development.  

---
## 4. Data Modelling

This project uses a simplified modelling approach designed to support the construction of a cloud based data pipeline while keeping the analytical workflow accessible and easy to interpret.  
The modelling decisions prioritise clarity, reproducibility and alignment with the scope of the work.

### 4.1 Medallion Architecture

The project follows the medallion architecture, which organises data into three refinement layers:

- **Bronze layer** containing raw ingested data exactly as loaded from source files.  
- **Silver layer** containing cleaned, standardised and quality assessed data, including corrections to naming, positions, duplicates and encoding issues.  
- **Gold layer** containing refined tables created specifically for answering the research questions.

This structure supports transparent data lineage and allows each transformation step to be documented and verified independently.

### 4.2 Data Structure Used

Within the gold layer, the project adopts a *flat table* structure.  
Each gold table contains all relevant attributes needed to support the analyses directly, without additional dimensional layers or multiple lookup tables.

This structure was selected because:
- The analytical questions can be answered using season level datasets without requiring dimensional modelling.  
- Flat tables reduce complexity and make the flow of data transformations easier to demonstrate.  
- The objective of the work is to show effective data engineering practices in Databricks rather than to optimise for OLAP performance.  
- The dataset is relatively small and does not demand the overhead of a star or snowflake schema.

The result is a set of gold tables that are compact and self contained.

---
## 5. Data Catalogue (Gold Layer)

The data catalogue describes the refined analytical tables produced in the gold layer of the medallion architecture.  
These tables contain the final attributes used to answer the research questions, together with information about their meaning, data type and typical value ranges.
Columns marked with * are not numerical and do not require min/max analysis.


### 5.1 Table: `nba_stats_gold`

This table contains season level statistical records for all NBA players from 1982 to 2022, after data cleaning, standardisation of player names and positions and removal of duplicated multi team aggregates (TOT entries).

| Column | Type | Description | Min | Max |
|-------|------|-------------|-----|-----|
| season | int | NBA season year | 1982 | 2022 |
| player_name* | string | Player’s full name | — | — |
| pos* | string | Normalised player position (PG, SG, SF, PF, C) | — | — |
| age | int | Player age during the season | 18 | 44 |
| team_abbrev* | string | Abbreviation of the player's team | — | — |
| g | int | Games played | 1 | 82 |
| gs | int | Games started | 0 | 82 |
| mp_per_g | double | Minutes played per game | 0.0 | 43.7 |
| trb_per_g | double | Rebounds per game | 0.0 | 18.7 |
| ast_per_g | double | Assists per game | 0.0 | 14.5 |
| stl_per_g | double | Steals per game | 0.0 | 3.7 |
| blk_per_g | double | Blocks per game | 0.0 | 5.6 |
| tov_per_g | double | Turnovers per game | 0.0 | 5.7 |
| pts_per_g | double | Points per game | 0.0 | 37.1 |
| fg_pct | double | Field goal percentage (0 to 1 scale) | 0.0 | 1.0 |
| fg3_pct | double | Three point field goal percentage (0 to 1 scale) | 0.0 | 1.0 |
| ft_pct | double | Free throw percentage (0 to 1 scale) | 0.0 | 1.0 |
| per | double | Player Efficiency Rating (a measure of per-minute production) | -90.6 | 133.8 |
| ts_pct | double | True Shooting Percentage (shooting efficiency considering 2-pt FG, 3-pt FG and FT) | 0.0 | 1.5 *(values above 1 inherited from original dataset)* |
| ws | double | Win Shares (an estimate of the number of wins contributed by a player) | -2.1 | 21.2 |
| award_share | double | MVP voting share | 0.0 | 1.0 |


### 5.2 Table: `mvp_stats_gold`

This table combines the MVP list with the corresponding cleaned per game statistics for each MVP season, enabling direct comparison of performance patterns among the award winners.

| Column | Type | Description | Min | Max |
|--------|------|-------------|------|------|
| season | int | Season in which the MVP was awarded | 1982 | 2022 |
| player_name* | string | MVP player name (cleaned and standardised) | — | — |
| pos | string | Normalised player position | C | SG |
| age | int | MVP age in the award season | 22 | 35 |
| stats_team_abbrev* | string | Team for which the season statistics were recorded | — | — |
| mvp_team_name* | string | Official team associated with the MVP award | — | — |
| mp_per_g | double | Minutes per game | 30.4 | 42.0 |
| pts_per_g | double | Points per game | 15.5 | 35.0 |
| trb_per_g | double | Rebounds per game | 3.3 | 15.3 |
| ast_per_g | double | Assists per game | 1.3 | 12.8 |
| stl_per_g | double | Steals per game | 0.5 | 3.2 |
| blk_per_g | double | Blocks per game | 0.1 | 3.7 |
| tov_per_g | double | Turnovers per game | 2.1 | 5.4 |
| fg_pct | double | Field goal percentage | 0.42 | 0.583 |
| fg3_pct | double | Three point percentage | 0.0 | 0.454 |
| ft_pct | double | Free throw percentage | 0.524 | 0.921 |
| per | double | Player Efficiency Rating | 22.0 | 32.8 |
| ts_pct | double | True Shooting Percentage | 0.518 | 0.669 |
| ws | double | Win Shares | 9.6 | 21.2 |
| award_share | double | MVP voting share | 0.691 | 1.0 |



### 5.3 Table: `mvp_champions_gold`

This table merges MVP winners with champion team results to enable cross comparison of individual and team success in the same season.

| Column | Type | Description | Min | Max |
|--------|------|-------------|------|------|
| season | int | Season year | 1982 | 2022 |
| player_name | string | MVP player name | — | — |
| pos | string | Player position | C | SG |
| age | int | MVP age | 22 | 35 |
| stats_team_abbrev | string | Team associated with season statistics | — | — |
| mvp_team_name | string | Team listed for MVP award | — | — |
| champion_team_name | string | Team that won the NBA championship | — | — |
| mvp_and_champion_same_team | int | Indicator showing whether the MVP and champion belonged to the same team (1 = yes) | 0 | 1 |


---

## 6. Summary of Data Transformations
This section summarises the main steps performed during the construction of the data pipeline.  
As mentioned before, the work follows the medallion architecture, with each layer introducing additional structure and refinement.

### 6.1 Bronze Layer
- Loaded all CSV files into a Databricks Volume.  
- Created bronze tables directly from the raw files, preserving all columns and values exactly as provided.  
- Performed initial inspection to confirm data types, column structures, and potential tranformation required.

The bronze layer acts as the immutable record of the input data.


### 6.2 Silver Layer
- Normalised column names for consistency.  
- Standardised player positions into a controlled set of categories (PG, SG, SF, PF, C).  
- Investigated and resolved cases where players appeared to exceed 82 games due to TOT (multiple team) records.  
- Verified the presence of accented characters in player names to confirm proper encoding.  
- Cleaned inconsistencies in the supplementary datasets, including duplicate franchise records.  
- Corrected mismatches between MVP names and player names in the main statistics table.  
- Filtered the data to include only seasons from 1982 to 2022, matching the scope of the primary dataset.

These updates produced a clean and reliable dataset for the gold layer.


### 6.3 Gold Layer
- Created analytical tables specifically designed to answer each research question.  
- Joined MVP, player statistics and champion data into combined structures where appropriate.  
- Built helper tables or views, such as a baseline of MVP averages, to support comparative evaluations.  
- Ensured that all tables in the gold layer followed a flat structure to simplify querying and improve clarity.

The gold layer is the primary input for all analysis in the project.


### 6.4 Data Quality Assessment

A structured data quality review was conducted to identify potential issues:
- Null values were counted for every column.  
- Columns with high concentrations of missing values, such as three point percentage in early seasons, were examined at the season level.  
- Outliers were checked, including unexpected game counts or negative performance metrics.  
- Duplicate records in the franchise dataset were detected and corrected.  
- Character encoding issues were investigated using string pattern detection.  
- Some anomalies were retained but documented due to their relevance or minor analytical impact.

Where appropriate, the findings were incorporated into the silver layer transformations.

---


## 7. Analysis Results

The full analytical process, including all SQL and PySpark commands, intermediate outputs and detailed exploratory steps, is available in the accompanying Databricks notebook `NBA_MVP_Pipeline.ipynb` stored in this repository.  
This section summarises the final results of the analysis, presenting the conclusions derived from the refined gold layer tables.

### 7.1 Question 1: Which statistics are most consistent among MVP winners?

#### 7.1.1 Overview

To identify the most representative performance indicators among MVP winners, the project calculated the minimum, average and maximum values for selected metrics.  
This included points per game, efficiency measures and impact-based statistics.

#### 7.1.2 Findings

The analysis showed that four metrics consistently characterise MVP level performance:

- **Points per game (PTS/G)** showed a narrow range for MVPs, with averages around the high twenties.  
- **Player Efficiency Rating (PER)** remained consistently high across all winners.  
- **Win Shares (WS)** reflected strong team impact and sustained contributions.  
- **True Shooting Percentage (TS%)** showed efficient scoring across nearly all MVP seasons.

These metrics varied less than others and formed a statistical baseline that distinguishes MVP seasons from typical high-performing seasons.

#### 7.1.3 Conclusion

MVP seasons tend to cluster around strong scoring, high efficiency and broad on-court impact.  
PTS/G, PER, TS% and WS collectively represent the most stable indicators of MVP level performance across the 1982 to 2022 seasons.


### 7.2 Question 2: What is the distribution of positions among MVP winners?

#### 7.2.1 Overview

A count was performed on the standardised positions of all MVP winners in the period.  
Positions were normalised into five categories: PG, SG, SF, PF and C.

#### 7.2.2 Findings

The distribution of MVP positions was:

- Power Forward (PF): most common  
- Point Guard (PG): second most common  
- Shooting Guard (SG), Centre (C) and Small Forward (SF): lower but still substantial representation

This reflects changes in playing style, league pace and the strategic importance of different positions over time.

#### 7.2.3 Conclusion

While PFs and PGs appear more frequently as MVPs, all five positions have produced winners.  
This indicates that elite performance is not restricted to any single role, even though certain positions have historically aligned more closely with MVP-type production.


### 7.3 Question 3: Do some teams historically concentrate more MVP awards?

#### 7.3.1 Overview

The number of MVP wins per franchise was counted to identify patterns of concentration.

#### 7.3.2 Findings

The teams with the greatest number of MVP seasons were:

- **Chicago Bulls**  
- **Los Angeles Lakers**

Both franchises showed extended periods of competitive dominance associated with iconic players.  

At the same time, **15 different franchises** had at least one MVP winner, demonstrating strong competitive distribution across the league.

#### 7.3.3 Conclusion

Although the Bulls and Lakers stand out, the overall spread of MVP awards across many franchises suggests a balanced and competitive environment.  
Periods of concentrated success tend to align with teams that sustained high performance and stability.


### 7.4 Question 4: How often does the season MVP play for the team that wins the NBA championship?

#### 7.4.1 Overview

A join between MVP and champion data identified seasons where both awards aligned.

#### 7.4.2 Findings

Out of forty-one seasons analysed, the MVP’s team also won the NBA championship fourteen times.  
This represents roughly one third of all seasons in scope.

This result indicates that strong regular season performance does not guarantee postseason success.  
Playoffs introduce different competitive dynamics, matchups and pressures.

#### 7.4.3 Conclusion

There is a moderate relationship between MVP performance and team championships, but the overlap is far from absolute.  
Individual excellence and team success often diverge due to the unique demands of playoff basketball.


### 7.5 Question 5: Which players delivered outstanding seasons without winning the MVP?

#### 7.5.1 Overview

A comparison was made between MVP baseline averages and non-MVP player performance.  
Players who exceeded a defined threshold across key metrics were identified.

#### 7.5.2 Findings

A strict requirement to exceed MVP averages in all selected metrics produced no matching players.  
A relaxed threshold of ninety percent identified a small group of players with exceptional seasons that were statistically close to MVP level.

Expanding the threshold to eighty percent widened the group but also reduced the strength of the comparison.

#### 7.5.3 Conclusion

Very few non-MVP players produced seasons that closely matched the statistical profile of MVP winners.  
This reinforces the idea that MVP level performance is extremely rare, and that even excellent players may fall short of the combination of efficiency, scoring and impact associated with the award.

---

## 8. Self Assessment

This section reflects on the extent to which the objectives of the project were achieved, the challenges encountered during the analytical process and future opportunities to strengthen or extend the work. The central goal of the project was to answer the research questions defined at the outset by preparing the data appropriately, ensuring its quality and applying clear and reproducible analytical methods.

### 8.1 Achievement of Objectives

The project successfully met its main objective.  
For each of the five research questions, the data was transformed, structured and analysed in a way that produced clear and defensible conclusions.  
This required identifying the relevant attributes, constructing analytical tables that represented the problem accurately and validating that the results were consistent with expectations from historical NBA trends.

The analyses relied on logic derived from the cleaned gold layer tables, ensuring that the answers were based on reliable data.  
The conclusions reflect meaningful insights into MVP level performance, positional trends, franchise dominance, the overlap between individual and team success and the identification of high performing players who did not win the MVP award.

Overall, the analytical objectives as defined in the project scope were achieved.

### 8.2 Challenges Encountered

A number of challenges emerged during the work:

- It was necessary to investigate data inconsistencies, such as TOT multi team cases or name mismatches, to avoid misleading analytical results.  
- Some metrics contained missing values or uneven historical coverage, which required contextual interpretation before being used in analysis.  
- Establishing a fair and justifiable threshold for identifying outstanding non MVP seasons required iterative refinement and reflection.  
- Defining a modelling approach that remained simple enough for the scope of the assignment while still supporting the analytical questions involved careful consideration.

These challenges contributed positively to the learning experience, reinforcing the importance of critical thinking in data preparation and analysis.

### 8.3 Limitations

Several limitations should be acknowledged:

- The analysis covers only seasons from 1982 to 2022, which limits historical comparisons to earlier periods.  
- The identification of outstanding non MVP players was based on a simplified threshold approach and could be expanded with more advanced statistical modelling.  
- Some data quality issues were noted but not fully explored due to time constraints.  
- Visual analysis was not included, even though graphs could enrich the interpretation of trends.  
- The flat gold tables provide clarity but limit more complex analytical modelling.

These limitations do not undermine the findings but indicate areas where deeper exploration could be beneficial.

### 8.4 Future Enhancements

To strengthen the work and its value in a portfolio, several extensions could be incorporated:

- Using predictive modelling to estimate MVP likelihood based on player performance metrics.  
- Introducing richer performance indicators such as BPM or VORP for more detailed comparisons.  
- Adding a visualisation layer to present distributions, trends and contrasts in a more intuitive format.  
- Expanding the temporal scope to include earlier decades and cross era comparisons.  
- Conducting a more extensive data quality audit, including statistical outlier detection and completeness profiling.  
- Automating the collection of supplementary datasets through APIs or scraping.

These improvements would enhance the analytical depth and provide a broader and more sophisticated understanding of MVP level performance in the NBA.

### 8.5 Note on the Use of AI

Artificial intelligence was used selectively to support this work. It assisted with:
- refining code snippets  
- improving clarity in the documentation  

All analytical decisions, validation steps and transformations were performed manually by the author, and the AI support did not substitute for technical reasoning or interpretation.

---

## 9. Final Notes

This project demonstrates a complete data engineering workflow developed in Databricks, including ingestion, cleaning, modelling and analytical exploration of NBA datasets.  
The medallion architecture provided a clear structure for documenting each transformation step and maintaining transparent data lineage.  
The gold layer enabled a direct connection between the refined datasets and the research questions, ensuring that each answer was grounded in reproducible queries.

The accompanying Databricks notebook contains all code, intermediate outputs, transformation logic, data quality checks and analytical queries used throughout the project.  
This ensures that every step of the pipeline can be revisited, validated or extended in future work.

While the project focused on educational objectives rather than production level optimisation, it highlights how cloud based tools such as Databricks support modern data workflows in a scalable and collaborative environment.

The insights derived from the analyses provide a historical perspective on MVP level performance and offer a foundation for more advanced modelling, predictive analytics or visualisation work.

