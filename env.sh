#!/bin/bash

# Note:
# There are generally only two parameters to change when
# moving the code elsewhere and changing the database:
# FORECASTER_HOME
# DB_NAME
# See below.

# This script contains basic environment parameters for the
# forecasting system. Additional parameters are found in
# Forecast.conf. The remaining configuration parameters are
# found in the accompanying database.

# Home
# Here is the only change required in the whole code
# to install it in another location.
FORECASTER_HOME=/root/madness_dev
#FORECASTER_HOME=/home/jorgeb/WorkUM/PathCostMetrics/madness_dev6
export FORECASTER_HOME

# Config path
CONF_PATH=$FORECASTER_HOME/conf
export CONF_PATH

# Log path
LOG_PATH=logs/
export LOG_PATH

# Drop-off point for raw data at destination site
DROP_POINT=/var/tmp
export DROP_POINT

# Pick-up point for raw data at source site
LOAD_POINT=/var/tmp
export LOAD_POINT

# Command line path extension
PATH=$FORECASTER_HOME/bin:$PATH:`dirname $0`
export PATH

# Perl libraries
PERL5LIB="$FORECASTER_HOME/lib":$PERL5LIB
export PERL5LIB

# Application
APP_PATH=$FORECASTER_HOME/bin
export APP_PATH
APP_USER=root
export APP_USER

# Database parameters
# Here is the only change required to use another database
DB_NAME=madCow
export DB_NAME
DB_USER=root
export DB_USER
DB_PASSWD=
export DB_PASSWD


RC=1

if [ $# -gt 0 ]
then

   while  [ "$RC" = 1 ];
   do
      RC=0
      $*
      RC=$?
   done

fi
