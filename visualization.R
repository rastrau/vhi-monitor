library(duckdb)
library(ggplot2)
library(dplyr)
library(geofacet)
library(patchwork)

tilemap_layout <- data.frame(
  code = c(31, 42, 53, 32, 37, 39, 43, 47, 54, 36, 33, 38, 40, 44, 51, 52, 68,
           55, 57, 34, 41, 45, 48, 49, 67, 66, 56, 59, 35, 61, 63, 46, 64, 50,
           58, 60, 62, 65),
  name = c("Eastern Jura", "Eastern Swiss Plateau", "Franches-Montagnes",
           "Balsthal", "Oberaargau", "Freiamt", "Lake Zurich Region",
           "Alpstein", "Western Jura", "Gruyère", "Lower Emmental",
           "Upper Emmental", "Western Central Switzerland",
           "Eastern Central Switzerland", "Rhine Valley", "Prättigau",
           "Lower Engadine", "Western Swiss Plateau", "Chablais",
           "Bernese Plateau", "Eastern Bernese Oberland", "Uri Alps",
           "Glarus Alps", "North Central Grisons", "Upper Engadine",
           "Grisons Southern Valleys", "Lake Geneva Region", "Northern Valais",
           "Western Bernese Oberland", "Goms", "Northern Ticino", "Surselva",
           "Eastern Ticino", "South-Central Grisons", "Valais Southern Alps",
           "Visp Valleys", "Western Ticino", "Sottoceneri"),
  row = c(1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4,
          4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6),
  col = c(4, 7, 3, 4, 5, 6, 7, 8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5,
          6, 7, 8, 9, 10, 1, 3, 4, 5, 6, 7, 8, 9, 4, 5, 7, 8),
  stringsAsFactors = FALSE
)

# geofacet::grid_preview(tilemap_layout)

# Connect to DuckDB database
con <- dbConnect(duckdb(), dbdir = "vhi.duckdb", read_only = TRUE)

# Tables:
# - forest_timeline
# - vegetation_timeline

# Attributes:
# - region (INTEGER)
# - date (DATE)
# - day_of_year (INTEGER)
# - vhi (DOUBLE)

# Get current year
current_year <- as.numeric(format(Sys.Date(), "%Y"))

df_current_year_daily <-
  dbGetQuery(con,
             sprintf(
               "SELECT * FROM forest_timeline WHERE date_part('year', %s) = %d",
               "date", current_year)
             )

df_previous_years_daily <-
  dbGetQuery(con,
             sprintf(
               "SELECT * FROM forest_timeline WHERE date_part('year', %s) < %d",
               "date", current_year)
  )

# Calculate percentiles for historical data
df_previous_years_daily_stats <- df_previous_years_daily %>%
  group_by(region, day_of_year) %>%
  summarise(
    p10 = quantile(vhi, 0.1, na.rm = TRUE),
    p90 = quantile(vhi, 0.9, na.rm = TRUE)
  )

# Combine datasets
plot_data_daily <- left_join(df_current_year_daily,
                             df_previous_years_daily_stats,
                             by = c("region", "day_of_year"))

# Create plot
plot_daily <- ggplot(plot_data_daily, aes(x = day_of_year)) +
  geom_ribbon(aes(ymin = p10, ymax = p90), fill = "orange", alpha = 0.5) +
  geom_line(aes(y = vhi), color = "darkred", linewidth = 1) +
  labs(title = "Current Year vs Historical Percentiles",
       x = "Day of year",
       y = "VHI") +
  theme_minimal() +
  facet_geo(~ region, grid = tilemap_layout)

# ----------------

df_current_year_weekly <- df_current_year_daily %>%
  mutate(week_of_year = as.integer(strftime(date, format = "%V"))) %>%
  group_by(region, week_of_year) %>%
  summarise(
    vhi = mean(vhi, na.rm = TRUE)
  )


# Calculate percentiles for historical data with weekly resolution
df_previous_years_weekly_stats <- df_previous_years_daily %>%
  mutate(week_of_year = as.integer(strftime(date, format = "%V"))) %>%
  group_by(region, week_of_year) %>%
  summarise(
    p10 = quantile(vhi, 0.1, na.rm = TRUE),
    p90 = quantile(vhi, 0.9, na.rm = TRUE)
  )


# Combine datasets
plot_data_weekly <- left_join(df_current_year_weekly,
                              df_previous_years_weekly_stats,
                              by = c("region", "week_of_year"))

# Create plot
plot_weekly <- ggplot(plot_data_weekly, aes(x = week_of_year)) +
  geom_ribbon(aes(ymin = p10, ymax = p90), fill = "orange", alpha = 0.5) +
  geom_line(aes(y = vhi), color = "darkred", linewidth = 1) +
  labs(title = "Current Year vs Historical Percentiles",
       x = "Week of year",
       y = "VHI") +
  theme_minimal() +
  facet_geo(~ region, grid = tilemap_layout)


plot_daily
plot_weekly

plot_daily + plot_weekly + plot_layout(ncol = 1)

plot_daily / plot_weekly

# Clean up connection
dbDisconnect(con)


