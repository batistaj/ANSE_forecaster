package Environment;

# Environment.pm
#
# This module is part of the Forecaster process. It
# reads environment variables and initiates a number
# of variables used by the forecasting code.


use strict;

use vars  qw(
             @ISA @EXPORT $VERSION

             $FORECASTER_HOME
             $DROP_POINT

             $APP_PATH $APP_USER
             $DB_NAME $DB_USER $DB_PASSWD
            );
#use DBI;
#use Logging;
#use SQL;

use Exporter;
$VERSION = 1.0;
@ISA = qw(Exporter);

@EXPORT = qw( 
             $FORECASTER_HOME
             $DROP_POINT

             $APP_PATH $APP_USER
             $DB_NAME $DB_USER $DB_PASSWD
            );


# Code env variables and starting configuration file
$FORECASTER_HOME = $ENV{FORECASTER_HOME};
#$CONF_PATH = $ENV{CONF_PATH};
#my $CONFIG_FILE = "$CONF_PATH/Forecast.conf";

$DROP_POINT = $ENV{'DROP_POINT'};
$APP_PATH   = $ENV{'APP_PATH'};
$APP_USER   = $ENV{'APP_USER'};
$DB_NAME    = $ENV{'DB_NAME'};
$DB_USER    = $ENV{'DB_USER'};
$DB_PASSWD  = $ENV{'DB_PASSWD'};

1;
