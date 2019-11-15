library(readr)
library(dplyr, warn.conflicts = FALSE)
library(purrr, warn.conflicts = FALSE)
library(stringr)

if (!file.exists("data-raw/sadc_2017_national.dat")) {
  download.file(
    "ftp://ftp.cdc.gov/pub/data/yrbs/sadc_2017/sadc_2017_national.dat",
    "data-raw/sadc_2017_national.dat"
  )
}

# Parse SPSS program to get variables, levels and labels ------------------

# Split into appropriate chunks
spss_lines <- read_lines("data-raw/2017_sadc_spss_input_program.sps")
line_delims <- c("DATA LIST FILE", "VARIABLE LABELS", "VALUE LABELS",
  "MISSING VALUES")
parts <- map(line_delims, ~ str_detect(spss_lines, .)) %>%
  reduce(`|`) %>%
  cumsum()
spss_parts <- split(spss_lines, parts) %>%
  set_names(c("preamble", "variables", "labels", "levels", "postamble"))

# Parse variables and positions
variables <-
  spss_parts[["variables"]] %>%
  str_match_all("(\\w+) (\\d+)\\-(\\d+)") %>%
  map_dfr(as_tibble, .name_repair = "unique") %>%
  set_names(c("string", "variable", "start", "end")) %>%
  mutate(variable = str_to_lower(variable))

# Parse labels
nrows <- length(spss_parts[["labels"]])
labels <- spss_parts[["labels"]][c(-1, -nrows)] %>%
  str_replace_all("\"", "") %>%
  str_split_fixed(" ", n = 2) %>%
  as_tibble(.name_repair = "unique") %>%
  set_names(c("variable", "label")) %>%
  mutate(variable = str_to_lower(variable)) %>%
  write_csv("data-raw/labels.csv")

variables <- variables %>%
  left_join(labels) %>%
  write_csv("data-raw/variables.csv")

# write out levels as-is --- parsed later
spss_parts[["levels"]] %>%
  write_lines("data-raw/levels.txt")


# Read in raw data --------------------------------------------------------

vars <- read_csv("data-raw/variables.csv")
types <- read_csv("data-raw/types.csv")

vars <- vars %>%
  left_join(types)

# useful for debugging data problems
raw_char <- read_fwf(
  "data-raw/sadc_2017_national.dat",
  col_positions = fwf_positions(vars$start, vars$end, vars$variable),
  col_types = paste(rep("c", nrow(vars)), collapse = "")
)

# actual data to work with
raw <- read_fwf(
  "data-raw/sadc_2017_national.dat",
  col_positions = fwf_positions(vars$start, vars$end, vars$variable),
  col_types = str_sub(vars$type, 1, 1) %>% str_c(collapse = ""),
  na = "."
)

# Factor levels -----------------------------------------------------------
parse_group <- function(lines) {
  var <- lines[2] %>% str_trim() %>% str_to_lower()

  pieces <- lines[-(1:2)] %>% str_split_fixed(" ", 2)
  levels <- pieces[, 1]
  labels <- pieces[, 2] %>% str_replace_all("['\"]", "")

  label <- function(x) {
    factor(x, levels = levels, labels = labels)
  }

  set_names(list(label), var)
}

levels <- read_lines("data-raw/levels.txt")
grp <- cumsum(levels == "/")

factorise <- levels %>%
  split(grp) %>%
  map(parse_group) %>%
  flatten()
factorise <- factorise[intersect(names(factorise), names(raw))]

survey <- raw
for (var in names(factorise)) {
  cat(".")
  survey[[var]] <- factorise[[var]](survey[[var]])
}

# Replace numeric 0's with NA ---------------------------------------------
replace_0 <- function(x) {
  x[x == 0] <- NA
  x
}
survey <- survey %>%
  mutate_if(is.numeric, replace_0)

# Convert dichotomous to logical ------------------------------------------

dichot <- survey %>% names() %>% str_detect("^qn")
survey[dichot] <- survey[dichot] %>% map(~ .x == 1)

# Variable labels ---------------------------------------------------------

survey[] <- map2(survey, vars$label, function(x, label) {
  if (is.na(label)) {
    x
  } else {
    structure(x, label = label)
  }
})

# Drop variables containing only missing values ---------------------------

all_missing <- function(x) all(is.na(x))

# There are more columns with only missing values in survey because
# a number of columns in raw only contained 0s

survey %>% map_lgl(all_missing) %>% which %>% names
survey <- survey %>% discard(all_missing)

# Save --------------------------------------------------------------------
usethis::use_data(survey, overwrite = TRUE)
