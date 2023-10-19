library(tidyverse)
library(readxl)
library("DBI")
library(arrow)

# Target courses
courses <- read_excel('source_codes.xlsx')
match_course <- courses %>% filter(!is.na(CALPADSDescriptor)) %>% pull(Code)

# Demographics - Note that prior to this stage, I've converted and combined the source csvs into a single Parquet file. The logic could be easily switch to import the CSV that is exported from CALPADS
# More information on EOY 8.1: https://documentation.calpads.org/Reports/EOY3New/Report8.1_StudentProfile_List%28EOY3%29/
eoy_81_a <- read_parquet(file = 'data/2017-2018_81_eoy_snap.parquet') %>%
  mutate(year = "2018")
eoy_81_b <- read_parquet(file = 'data/2022-2023_81_eoy_snap.parquet') %>%
  mutate(year = "2023")
eoy_81_2023 <- bind_rows(eoy_81_a, eoy_81_b) %>%
  group_by(year,`SSID`) %>%
  mutate(ExitDate = as.Date.character(ExitDate, "%m/%d/%Y"), id = paste(year,SSID, sep = '|')) %>%
  arrange(year, SSID, desc(ExitDate)) %>%
  mutate(seq = row_number(id)) %>%
  filter(seq == 1)

# Pull Course Enrollment
# More information on 3.11: https://documentation.calpads.org/Reports/EOY1/Report3.11_CourseSectionsCompletedStudentList/
# Note that the 3.11 report is TERRIBLE and slow. It might require splitting it into multiple subreports by course type, or for a subset of schools to facilitate

course_2023 <- read_parquet(file = 'data/2022-2023_311_snap.parquet') %>%
  mutate(year = "2023", .before = everything())
course_2018 <- read_parquet(file = 'data/2017-2018_311_snap.parquet') %>%
  mutate(year = "2018", .before = everything())
grant_course <- course_2023 %>% 
  bind_rows(course_2018) %>%
  filter(
    substr(StateCourse,1,4) %in% match_course,
    AcademicTerm %in% c("FY", "Q4", "S2", "T3", "H6")
  ) %>%
  select(
    year,
    StateCourse,
    CourseContentAreaSubcategory,
    SSID,
    Gender,
    FedEnctyRaceCatgName,
    PrimaryDisabilityCode
  ) %>%
  mutate(
    id = paste(year,SSID, sep = '|'),
    SWD = if_else(PrimaryDisabilityCode == "", FALSE, TRUE),
    keep = case_when(
      !(substr(StateCourse,1,4) %in% c("9381", "9222")) ~ TRUE,
      substr(StateCourse,1,4) == "9381" & substr(CourseContentAreaSubcategory,1,7) == "MDAR-04" ~ TRUE,
      substr(StateCourse,1,4) == "9381" & substr(CourseContentAreaSubcategory,1,7) == "MDAR-05" ~ TRUE,
      substr(StateCourse,1,4) == "9222" & substr(CourseContentAreaSubcategory,1,7) == "STEM-04" ~ TRUE,
      substr(StateCourse,1,4) == "9222" & substr(CourseContentAreaSubcategory,1,7) == "STEM-09" ~ TRUE,
      TRUE ~ FALSE
    )
  ) %>%
  filter(
    keep == TRUE
  ) %>%
  left_join(eoy_81_2023, by = "id", suffix = c("", ".y")) %>%
  mutate(
    SED = if_else(SocioEconomicallyDisadvantaged == "Y", TRUE, FALSE),
    EL = if_else(EnglishLearner == "Y", TRUE, FALSE)
  )

grant_summary_groups <- grant_course %>%
  group_by(year, StateCourse, CourseContentAreaSubcategory, Gender) %>%
  summarise(
    Disability = sum(SWD),
    SED = sum(SED),
    EL = sum(EL)
  )

grant_summary_ethnicity <- grant_course %>%
  pivot_longer(
    cols = c(FedEnctyRaceCatgName)
  ) %>%
  group_by(year, StateCourse, CourseContentAreaSubcategory, Gender, value) %>%
  summarise(
    n = n()
  ) %>%
  pivot_wider(
    names_from =  "value",
    values_from = "n"
  ) %>%
  mutate(
    Total = rowSums(across(c(`Asian, Not Hispanic`:`Not Reported`)), na.rm = TRUE),
    .after = Gender
  )


combined_data <- grant_summary_ethnicity %>%
  left_join(grant_summary_groups)

write_excel_csv(combined_data, "SCALEData.csv", na = "")
