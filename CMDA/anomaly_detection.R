library(isotree)
library(recipes)
library(EnvStats)
library(dbscan)
library(MASS)
library(dplyr)
library(tidyr)
library(readr)
library(forcats)
library(readxl)
library(ggplot2)
library(lubridate)
library(purrr)

data_cols <- c('uuid', 'start', 'end', 'enum_id', 'binfo_inp_enum_username', 'h_type_inp',
               'hhr_inp_relation_with_pr', 'hhr_inp_gender', 'hhr_age', 
               'hhr_edn_status', 'hhr_edn_lvl', 'hhr_edn_fld_g', 'hhr_edn_fld_d',
               'hhr_work_yn', 'hhr_not_working', 'hhr_better_commute',
               'hhr_emp_status', 'hhr_occ_seo', 'hhr_occ_see', 'hhr_occ_gov',
               'hhr_occ_pvt', 'hhr_occ_wage', 'hhr_occ_gig', 'hhr_occ_inf',
               'hhr_ind', 'hhr_manuf', 'hhr_entsize',
               'hhr_benefits_see_1', 'hhr_benefits_see_2', 'hhr_benefits_see_3',
               'hhr_benefits_see_4', 'hhr_benefits_see_9998', 'hhr_benefits_see_9999',
               'hhr_ent_type', 'hhr_job_contract',
               'hhr_benefits_emp_1', 'hhr_benefits_emp_2', 'hhr_benefits_emp_3', 
               'hhr_benefits_emp_4', 'hhr_benefits_emp_9998', 'hhr_benefits_emp_9999',
               'hhr_paid_leave', 'hhr_entsize_emp', 'hhr_se_income', 'hhr_salary',
               'hhr_gig_income', 'hhr_gig_days', 'hhr_wage_days', 'hhr_wage_earnings',
               'hhr_freq_pymnt', 'hhr_freq_pymnt_others', 'hhr_inf_earnings_w',
               'hhr_inf_earnings_bm', 'hhr_inf_earnings_m', 'hhr_inf_earnings_o',
               'hhr_commute_days', 'hhr_commute_location', 'hhr_work_dist', 
               'hhr_1wtime', 'hhr_work_dist_mult', 'hhr_time_mult', 
               'hhr_mode_travel_1', 'hhr_mode_travel_2', 'hhr_mode_travel_3', 
               'hhr_mode_travel_4', 'hhr_mode_travel_5', 'hhr_mode_travel_6', 
               'hhr_mode_travel_7', 'hhr_mode_travel_8', 'hhr_mode_travel_9', 
               'hhr_mode_travel_10', 'hhr_mode_travel_11', 'hhr_mode_travel_12', 
               'hhr_mode_travel_13', 'hhr_mode_travel_14', 'hhr_mode_travel_9998', 
               'hhr_mode_travel_9999', 'hhr_travel_cost', 'hhr_travel_subsidy', 
               'hhr_travel_difficulty_yn', 'hhr_travel_difficulty_1', 'hhr_travel_difficulty_2',
               'hhr_travel_difficulty_3', 'hhr_travel_difficulty_4', 'hhr_travel_difficulty_5',
               'hhr_travel_difficulty_6', 'hhr_travel_difficulty_7', 'hhr_travel_difficulty_8',
               'hhr_travel_difficulty_9', 'hhr_travel_difficulty_9997', 
               'hhr_travel_difficulty_9998', 'hhr_travel_difficulty_9999', 
               'hi_hhsize', 'hi_religion', 'hi_caste', 'hi_ten_status', 
               'hi_rent', 'hi_rent_001', 'hi_mig_status', 'hi_prevloc',
               'hi_mig_reason_1', 'hi_mig_reason_2', 'hi_mig_reason_3', 'hi_mig_reason_4',
               'hi_mig_reason_5', 'hi_mig_reason_6', 'hi_mig_reason_7', 'hi_mig_reason_8',
               'hi_mig_reason_9', 'hi_mig_reason_10', 'hi_mig_reason_11', 'hi_mig_reason_9997',
               'hi_mig_reason_9998', 'hi_mig_reason_9999', 'hi_rooms', 'hi_roof',
               'hi_sep_kitch', 'hi_sep_toilet', 'hi_toilet_n', 
               'hi_loc_imp_1', 'hi_loc_imp_2', 'hi_loc_imp_3', 'hi_loc_imp_4',
               'hi_loc_imp_5', 'hi_loc_imp_6', 'hi_loc_imp_9997', 'hi_loc_imp_9998',
               'hi_loc_imp_9999', 'hi_assets_fan', 'hi_assets_taxaut',
               'hi_assets_tv', 'hi_assets_fridge', 'hi_assets_inverter', 'hi_assets_ro',
               'hi_assets_farmland', 'hi_assets_property', 'hi_assets_2w', 'hi_assets_sp',
               'hi_assets_bp', 'hi_assets_lptp', 'hi_assets_ac', 'hi_assets_car', 
               'hi_assets_cattle', 'hhr_other_income_1', 
               'hhr_other_income_2', 'hhr_other_income_3', 'hhr_other_income_4',
               'hhr_other_income_5', 'hhr_other_income_9997', 'hhr_other_income_9998',
               'hhr_other_income_9999', 'hhr_other_income_amt',
               'duration')

data_cols_cat <- c('h_type_inp', 'hhr_inp_relation_with_pr', 'hhr_inp_gender',  
                   'hhr_edn_status', 'hhr_edn_fld_g', 'hhr_edn_fld_d',
                   'hhr_work_yn', 'hhr_not_working', 'hhr_better_commute',
                   'hhr_emp_status', 'hhr_occ_seo', 'hhr_occ_see', 'hhr_occ_gov',
                   'hhr_occ_pvt', 'hhr_occ_wage', 'hhr_occ_gig', 'hhr_occ_inf',
                   'hhr_ind', 'hhr_manuf', 
                   'hhr_ent_type', 'hhr_job_contract',
                   'hhr_paid_leave', 'hhr_travel_subsidy', 
                   'hhr_travel_difficulty_yn', 'hi_religion', 'hi_caste', 'hi_ten_status', 
                   'hi_mig_status', 'hi_prevloc',
                   'hi_roof', 'hi_toilet_n')

detect_record_anomalies <- function(data) {
  numeric_data <- data %>% 
    dplyr::select(-c(uuid, enum_id, binfo_inp_enum_username)) %>% 
    dplyr::select(where(is.numeric))
  
  numeric_scaled <- scale(numeric_data)
  
  categorical_data <- data %>% 
    dplyr::select(-c(uuid, enum_id, binfo_inp_enum_username)) %>% 
    dplyr::select(where(is.factor))
  
  if(ncol(categorical_data) > 0) {
    dummy_matrix <- model.frame(~ . - 1, data = categorical_data, na.action = na.pass)
    dummy_matrix <- model.matrix(~ . - 1, dummy_matrix)
    
    dummy_df <- as_tibble(dummy_matrix) %>% 
      dplyr::select(where(function(x){var(x)>0}))
    
    combined_data <- cbind(numeric_scaled, dummy_df)
  } else {
    combined_data <- numeric_scaled
  }
  
  iso <- isolation.forest(combined_data, 
                          ntrees = 500,
                          ndim = 2,
                          sample_size = nrow(combined_data) %/% 3 * 2, 
                          prob_pick_avg_gain = 1,
                          nthreads = parallel::detectCores() - 1,
                          seed = 10)
  
  iso_scores <- predict(iso, combined_data)
  
  lof_scores <- lof(combined_data, k = min(20, nrow(data) - 1))
  
  normalize_scores <- function(x) {
    (x - min(x)) / (max(x) - min(x))
  }
  
  record_anomalies <- data %>%
    mutate(
      iso_score = normalize_scores(iso_scores),
      lof_score = normalize_scores(lof_scores),
      
      is_anomalous = iso_score > quantile(iso_score, 0.95) |
                     lof_score > quantile(lof_score, 0.95)
     )
    
  enumerator_summary <- record_anomalies %>%
    group_by(enum_id) %>%
    summarise(
      n_surveys = n(),
      n_anomalous = sum(is_anomalous),
      anomaly_rate = round(mean(is_anomalous) * 100, 1),
      mean_iso_score = mean(iso_score),
      mean_lof_score = mean(lof_score),
      iso_anomalies = sum(iso_score > quantile(record_anomalies$iso_score, 0.95)),
      lof_anomalies = sum(lof_score > quantile(record_anomalies$lof_score, 0.95)),
    )
  
  return(list(
    record_anomalies = record_anomalies,
    enumerator_summary = enumerator_summary
  ))
}

identify_contributing_variables <- function(record, data) {
  # Compare each variable's value to its distribution
  contrib_vars <- map_dfr(names(data), function(var) {
    val <- record[[var]]
    dist <- data[[var]]
    z_score <- (val - mean(dist, na.rm = TRUE)) / sd(dist, na.rm = TRUE)
    
    tibble(
      variable = var,
      z_score = abs(z_score)
    )
  }) %>%
    filter(z_score > 2) %>%  # More than 2 standard deviations away
    arrange(desc(z_score))
  
  if(nrow(contrib_vars) > 0) {
    paste(contrib_vars$variable, collapse = ", ")
  } else {
    "No strong individual contributors"
  }
}

detect_distribution_anomalies <- function(data) {
  compute_distribution_scores <- function(data, variable) {
    if(variable == "hi_caste") {
      overall_dist <- tibble(
        hi_caste = c(-9999, -9998, -9997, 1, 2, 3, 4, 5),
        overall_prop = c(0.06, 0.01, 0.05, 0.01, 0.15, 0.65, 0.07, 0.0)
      )
    } else {
      overall_dist <- data %>%
        mutate(!!variable := as.numeric(as.character(!!sym(variable)))) %>% 
        count(!!sym(variable)) %>%
        mutate(overall_prop = n / sum(n)) %>%
        dplyr::select(-n)
    }
    
    enum_dist <- data %>%
      group_by(enum_id) %>%
      mutate(!!variable := as.numeric(as.character(!!sym(variable)))) %>% 
      count(!!sym(variable)) %>%
      mutate(enum_prop = n / sum(n)) %>%
      left_join(overall_dist, by = variable) %>%
      mutate(
        diff_from_expected = abs(enum_prop - overall_prop),
        chi_sq_contrib = (n - sum(n) * overall_prop)^2 / (sum(n) * overall_prop)
      )
    
    enum_scores <- enum_dist %>%
      group_by(enum_id) %>%
      summarise(
        kl_divergence = sum(enum_prop * log(enum_prop / overall_prop), na.rm = TRUE),
        max_category_deviation = max(diff_from_expected),
        chi_sq_stat = sum(chi_sq_contrib),
        n_responses = sum(n),
        unusual_categories = list(
          enum_dist %>%
            filter(diff_from_expected > quantile(diff_from_expected, 0.95)) %>%
            pull(!!sym(variable))
        )
      )
    
    return(enum_scores)
  }
  
  cat_vars <- names(dplyr::select(data, where(is.factor)))
  distribution_scores <- map_df(cat_vars, function(var) {
    scores <- compute_distribution_scores(data, var) %>%
      mutate(variable = var)
    return(scores)
  })
  
  numeric_distribution_scores <- data %>%
    dplyr::select(enum_id, where(is.numeric)) %>%
    gather(variable, value, -enum_id) %>%
    group_by(enum_id, variable) %>%
    summarise(
      ks_stat = ks.test(
        value, 
        y = data[[cur_group()$variable[1]]]
      )$statistic,
      mean_diff = abs(mean(value, na.rm = TRUE) - 
                     mean(data[[cur_group()$variable[1]]], na.rm = TRUE)),
      sd_ratio = sd(value, na.rm = TRUE) / 
                 sd(data[[cur_group()$variable[1]]], na.rm = TRUE),
      distinct_ratio = n_distinct(value) / n(),
      zero_prop = mean(value == 0, na.rm = TRUE),
      round_prop = mean(value == round(value), na.rm = TRUE),
      unusual_values = list(boxplot.stats(value)$out)
    )
  
  return(list(
    categorical = distribution_scores,
    numeric = numeric_distribution_scores
  ))
}

combine_anomaly_scores <- function(individual_scores, distribution_scores) {
  combined <- individual_scores$enumerator_summary %>%
    left_join(
      distribution_scores$categorical %>%
        group_by(enum_id) %>%
        summarise(
          mean_kl_divergence = mean(kl_divergence),
          max_category_deviation = max(max_category_deviation),
          mean_chi_sq = mean(chi_sq_stat),
          problematic_categories = list(unique(unlist(unusual_categories)))
        ),
      by = "enum_id"
    ) %>%
    left_join(
      distribution_scores$numeric %>%
        group_by(enum_id) %>%
        summarise(
          mean_ks_stat = mean(ks_stat),
          mean_moment_diff = mean(mean_diff),
          mean_sd_ratio = mean(sd_ratio),
          problematic_numerics = list(
            names(which(mean(sapply(unusual_values, length) > 0) > 0.1))
          )
        ),
      by = "enum_id"
    ) %>%
    mutate(
      iso_risk = scale(mean_iso_score)[,1],
      lof_risk = scale(mean_lof_score)[,1],
      distribution_risk = scale(mean_kl_divergence)[,1] + 
                         scale(max_category_deviation)[,1] + 
                         scale(mean_ks_stat)[,1],
      
      is_suspicious_iso = iso_risk > 1.96,
      is_suspicious_lof = lof_risk > 1.96,
      is_suspicious_distribution = distribution_risk > 1.96,
      
      is_suspicious = is_suspicious_iso | is_suspicious_lof | 
                     is_suspicious_distribution
    )
  
  return(combined)
}

hh_df <- read_csv("Data/CMDA_SDEIC_HH_main.csv")
roster_df <- read_csv("Data/CMDA_SDEIC_HH_roster.csv")
roster_df <- roster_df %>% 
  left_join(hh_df, by = "uuid") 

data_key_vars <- roster_df %>% 
  mutate(duration = mdy_hm(roster_df$end) - mdy_hm(roster_df$start)) %>% 
  dplyr::select(all_of(data_cols)) %>% 
  mutate(
    across(all_of(data_cols_cat), as.factor),
    across(where(is.factor), function(x){fct_na_value_to_level(x, "Missing")}),
    across(where(is.numeric), function(x){if_else(is.na(x) | x < -9996, 0, x)})
  )

individual_anomalies <- detect_record_anomalies(data_key_vars)

data_cols_cat_addnl <- c("hhr_edn_lvl", "hhr_benefits_see", "hhr_benefits_emp", "hhr_se_income",
  "hhr_salary", "hhr_gig_income", "hhr_wage_earnings",
  "hhr_freq_pymnt", "hhr_commute_days", "hhr_commute_location",
  "hhr_mode_travel", "hhr_travel_difficulty", "hi_mig_reason", 
  "hi_sep_kitch", "hi_sep_toilet", "hi_loc_imp", "hi_assets_fan", "hi_assets_taxaut",
  "hi_assets_tv", "hi_assets_fridge", "hi_assets_inverter",       
  "hi_assets_ro", "hi_assets_farmland", "hi_assets_property",        
  "hi_assets_2w", "hi_assets_sp", "hi_assets_bp", 
  "hi_assets_lptp", "hi_assets_ac", "hi_assets_car",             
  "hi_assets_cattle", "hhr_other_income")

data_key_vars_distr <- roster_df %>% 
  mutate(duration = mdy_hm(roster_df$end) - mdy_hm(roster_df$start)) %>% 
  dplyr::select(all_of(union(data_cols, data_cols_cat_addnl))) %>% 
  mutate(
    across(all_of(union(data_cols_cat, data_cols_cat_addnl)), as.factor),
    across(where(is.factor), function(x){fct_na_value_to_level(x, "Missing")}),
    across(where(is.numeric), function(x){if_else(is.na(x) | x < -9996, 0, x)})
  )

distribution_anomalies <- detect_distribution_anomalies(data_key_vars_distr)
combined_scores <- combine_anomaly_scores(individual_anomalies, 
                                          distribution_anomalies)

enum_summary_manual <- roster_df %>% 
  mutate(duration = mdy_hm(roster_df$end) - mdy_hm(roster_df$start)) %>% 
  group_by(enum_id, binfo_inp_enum_username) %>% 
  summarise(
    Duration_15_or_20_min = sum(duration == 15 | duration == 20) / n(),
    Industry_Other = sum(hhr_ind == -9997, na.rm = TRUE) / n(),
    Industry_ArtRec =  sum(hhr_ind == 12, na.rm = TRUE) / n(),
    Caste_ST = sum(hi_caste == 1) / n(),
    Caste_SC = sum(hi_caste == 2) / n(),
    Caste_ST_Muslim = sum(hi_caste == 1 & hi_religion == 2) / n(),
    Caste_ST_Christian = sum(hi_caste == 1 & hi_religion == 3) / n(),
    Caste_SC_Muslim = sum(hi_caste == 2 & hi_religion == 2) / n(),
    Caste_SC_Christian = sum(hi_caste == 2 & hi_religion == 3) / n(),
    HType_Villa = sum(h_type_inp == 3) / n(),
    surveys = n()
  )

write_csv(enum_summary_manual, 'anomaly/manual_summary_enum.csv')
write_csv(individual_anomalies$record_anomalies, 'anomaly/rec_anomalies.csv')
write_csv(individual_anomalies$enumerator_summary, 'anomaly/rec_anomalies_enum.csv')
write_csv(distribution_anomalies$categorical, 'anomaly/dist_anomalies_cat.csv')
write_csv(distribution_anomalies$numeric, 'anomaly/dist_anomalies_num.csv')
write_csv(combined_scores, 'anomaly/automated_anomalies_enum.csv')


