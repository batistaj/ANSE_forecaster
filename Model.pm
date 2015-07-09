package Model;

# Model.pm
#
# This module is part of the forecasting system.
# It contains code related to interacting with R.

# TODO
# - Add that this requires forecast_5.8+, R version 3.1.2+, etc

use strict;
use warnings;

use Environment;
use Statistics::R;

use vars qw(@ISA @EXPORT $VERSION);

use Exporter;
$VERSION = 1.0;
@ISA = qw(Exporter);

@EXPORT = qw( &model
            );


#---

sub model {
    my ($R,$model,$base,$cycles,$str) = @_;

    my @sets = pair_sort($str);

    my @groups = ();
    foreach my $pair (@sets) {
        my @pair = [@$pair[0],@$pair[1]];
        unshift @groups, @pair;
    }

    # Obtain last entry (by using [-1])
    # last_time for edges with >$base recs; first_time for edges with $base recs.
    my $ltime = $groups[-1][0];
    my $value = $groups[-1][1];

    # Output data
    my $set = undef;
    foreach my $line (@groups) {
        $set .= "@$line\n";
    }

    my $start = 0;
    my $end = $base -1;

    # Pass input values to the R script, execute it, and generate forecasts
    $R->set( 'base', $base );
    $R->set( 'start', $start );
    $R->set( 'end', $end );
    $R->set( 'cycles', $cycles );
    $R->set( 'set', $set );

    my $out = $R->run_from_file( "$APP_PATH/$model.R" );
    #print "\$out: $out\n"; ### REMOVE
    if ( $out =~ m/error/i ) {
        print "Stopping R\n";
        exit(0);
    }

    # Obtain the results
    my $array_ref = $R->get( 'rows' );

    # Display the results of the forecasts
    $array_ref = $R->get( 'flist' );
    my $forecasts = get_next_prediction($array_ref,$cycles);

    return ($ltime, $forecasts);
}


# This function sorts time-forecast value pairs
# by descending order of time.
sub pair_sort {
    my $str = shift;

    my @set=split(',',$str);
    my $key = undef;
    my $val = undef;
    my @arr = ();
    my $i=1;
    foreach my $item (@set) {
        if ( $i % 2 ) {               # First in pair (time)
            $key = $item
        } else {                      # Last in pair (forecast value)
            $val = $item;
            my @elem = [$key,$val];
            push @arr, @elem;
        }
        $i++;
    }


    # Sort in descending order
    my @pairs = sort {$b->[0] <=> $a->[0]} @arr;

    return @pairs;
}


# This function prints the raw data obtained from the database
sub print_rawd {
    my ($array_ref,$start,$end,$cycles) = @_;

    my $rows = scalar (@$array_ref);
    my $forecast_points = 10;
    my $item = $start;
    for ( my $i=0; $i<$rows; $i++ ) {
        if ($i % 3 == 1) {
            print ",@$array_ref[$i]";
            # These are the values to be forecast
            if ( $item>$start+2+$end && $item<$start+$end+1+$cycles+$forecast_points ) {
                print " *";
            }
        } elsif ($i % 3 == 2) {
        } else {
            print "\n$item, @$array_ref[$i]";
            $item++;
        }
    }
}


# This function prints the results of fcast
sub print_fcast {
    my $array_ref = shift;
    # Note: Setting $r->get here would cause this error to display:
    # (in cleanup) Internal error: could not get STATE from IPC::Run
    # Therefore, it is done outside the sub.
    #my $array_ref = $R->get( 'fcast' );
    my $helem = 11;   # The number of elements in the header
    my $belem = 71;   # The number of elements in the block
    my $numcols = 6;  # The number of columns displayed
    my $size = scalar (@$array_ref);
    my $cycle = $size/$belem;
    for (my $i=0; $i<$cycle; $i++) {
        # Print header
        print "\n@$array_ref[0 + $i*$belem]\n@$array_ref[1 + $i*$belem]\t@$array_ref[2 + $i*$belem]\t" .
              "@$array_ref[3 + $i*$belem] @$array_ref[4 + $i*$belem]\t\t@$array_ref[5 + $i*$belem] @$array_ref[6 + $i*$belem]\t\t" .
              "@$array_ref[7 + $i*$belem] @$array_ref[8 + $i*$belem]\t\t@$array_ref[9 + $i*$belem] @$array_ref[10 + $i*$belem]";
        # Print the data
        for (my $j=$helem + $i*$belem; $j<$belem + $i*$belem; $j++) {
            if ( ($j+$i+1) % $numcols ) {
                print "@$array_ref[$j]\t";
            } else {
                print "\n@$array_ref[$j]\t";
            }
        }
        print "\n";
    }
}


# This function returns only the next predicted value
sub get_next_prediction {
    my ($array_ref,$cycles) = @_;

    my $fcasts = @$array_ref[12];
    for (my $i=1;$i<$cycles;$i++) {
        $fcasts .= ",@$array_ref[12+$i*71]";
    }
    return $fcasts;
}

1;
