setwd("/usr/lib64/R/library")
library("timeDate", lib.loc="/usr/lib64/R/library")
library("zoo",      lib.loc="/usr/lib64/R/library")
library("xts",      lib.loc="/usr/lib64/R/library")
library("forecast", lib.loc="/usr/lib64/R/library")

# Sample using ETS, which may not be a good model for this problem set.
# An analysis will be required.

# base   - The number of history points to use
# start  - The first record to use from the historical data source
# end    - The last records to use from the historical data source
# cycles - The number of forecasts to generate
# set    - The set of historical points from which to generate the forecasts

# ---
# See http://search.cpan.org/~fangly/Statistics-R/lib/Statistics/R.pm for details
# Especially:
#    suppressPackageStartupMessages(library(library_to_load))
# Note that R imposes an upper limit on how many characters can be contained on a line:
# about 4076 bytes maximum. You will be warned if this occurs. Commands containing lines
# exceeding the limit may fail with an error message stating:

#  '\0' is an unrecognized escape in character string starting "...

# If possible, break down your R code into several smaller, more manageable statements.
# Alternatively, adding newline characters "\n" at strategic places in the R statements
# will work around the issue.
# ---

# Actually, adding this suppresses desired output as well. Why?
# suppressPackageStartupMessages(library(library_to_load))

# Query and read the results
rows <- read.table(text=set)

# Forecasts are to be stored in flist as a list
flist = list()

# Call the forecasting model
for (i in 1:cycles) {
    start <- start + 1
    end <- end + 1
    tput <- ts(rows[start:end,2], frequency=1, start=c(start))
    fit <- ets(tput)
    fcast <- forecast(fit)
    flist[[i]] <- fcast
}
