package SQL;

# SQL.pm
#
# This module is part of the forecasting system.
# It serves as a repository of all the SQL used
# to work with the corresponding MySQL database.

# TODO
# - Rename path -> edge
# - Confirm repository code and sql is used throughout.
# - Check get_unnamed_nodes and why sometimes node_name='reache'.

use strict;
#use Time::gmtime;
#use Time::Local;

use vars qw(@ISA @EXPORT $VERSION);

#use Logging;
use Exporter;
$VERSION = 1.0;
@ISA = qw(Exporter);

@EXPORT = qw( &get_model_statuses

              &get_block_str
              &insert_prediction
              &delete_old_forecasts
              &mark_measure_as_processed
              &drop_path_counts
              &drop_null_counts
              &drop_path_new_entries
              &drop_path_latest_entries
              &drop_path_base_counts
              &drop_edges_wo_new_entries
              &drop_edges_not_done
              &get_path_counts
              &get_null_counts
              &get_edges_with_new_entries
              &get_latest_path_times
              &get_path_base_counts
              &get_edges_wo_new_entries
              &get_edges_not_done
              &get_latest_history
              &get_earliest_history
              &get_unmapped_edges
              &get_catchup_history
              &insert_map
              &get_fcast
              &get_new_measure_records
              &update_fcast

              &load_archive
              &delete_from_staging_archive

              &get_unnamed_nodes
              &insert_node_name
              &get_nodes
              &insert_nodes

              &get_archive_nodes
              &get_new_staging_records
              &insert_archive
              &mark_staging_records
            );


# ______________________________________________
#
# For the forecast generation process

# Identify which models to use in forecast generation
sub get_model_statuses {
    return "SELECT m.model_name,a.archive_name,g.group_name
            FROM models_registry m, archives_registry a, groups_registry g, status ms, status rs
            WHERE m.archive_id=a.id AND m.group_id=g.id AND m.status_id=ms.id AND a.status_id=rs.id
            AND ms.status='Enabled' AND rs.status='Enabled';";
}


# ______________________________________________
#
# For the roll process


# This returns the time block string to use as a clause for
# subsequent queries and the time_block to use as the array
# element for localtime/gmtime.
sub get_block_str {
    return "SELECT clause AS block_str, time_block FROM groups_registry WHERE group_name=?;";
}


sub insert_prediction {
    my ($forecast_model) = @_;

    return "INSERT IGNORE INTO $forecast_model
            (fcast,src_id,dst_id,last_time,grouping,new,base,sequence)
            VALUES (?,?,?,?,?,?,?,?);";
}


sub delete_old_forecasts {
    my ($forecast_model) = @_;

    return "DELETE FROM $forecast_model
            WHERE src_id=?
            AND dst_id=?
            AND grouping=?
            AND new=1;";
}


sub mark_measure_as_processed {
    my ($measure) = @_;

    return "UPDATE archive_$measure SET new=0 WHERE new=1;";
}


sub drop_path_counts {
    return "DROP TABLE IF EXISTS c;";
}


sub drop_path_new_entries {
    return "DROP TABLE IF EXISTS n;";
}


sub drop_path_latest_entries {
    return "DROP TABLE IF EXISTS a;";
}


sub drop_path_base_counts {
    return "DROP TABLE IF EXISTS e;";
}


sub drop_edges_wo_new_entries {
    return "DROP TABLE IF EXISTS d;";
}


sub drop_null_counts {
    return "DROP TABLE IF EXISTS v;";
}


sub drop_edges_not_done {
    return "DROP TABLE IF EXISTS g;";
}


# Obtain the number of records by path-block
sub get_path_counts {
    my ($block_str,$measure,$base) = @_;

    my $query =
        "CREATE TEMPORARY TABLE
             c ( INDEX(src_id,dst_id) )
         ENGINE=MEMORY
         AS (SELECT src_id,dst_id,
                    $block_str block,
                    count(*) tot
             FROM archive_$measure
             GROUP BY src_id,dst_id,block
             HAVING tot>$base
            );";

    return $query;
}


# For each path-block with more than 10 entries,
# count the number entries with no mapping.
sub get_null_counts {
    my ($block_str,$measure,$forecast_map,$forecast_model,$base) = @_;

    my $query =
        "CREATE TEMPORARY TABLE
                     v ( INDEX(src_id,dst_id) )
                 ENGINE=MEMORY
                 AS (SELECT t.src_id,t.dst_id,
                            $block_str block,
                            count(*) nulltot
                     FROM archive_$measure t
                          LEFT JOIN
                          ($forecast_map m,$forecast_model f)
                     ON (t.id=m.archive_id AND m.model_id=f.id)
                     WHERE m.archive_id IS NULL
                     GROUP BY t.src_id,t.dst_id,block
                     HAVING nulltot>$base
                    );";

    return $query;
}


sub get_edges_with_new_entries {
    my ($block_str,$measure) = @_;

    my $query =
        "CREATE TEMPORARY TABLE
             n ( INDEX(src_id,dst_id) )
         ENGINE=MEMORY
         AS (SELECT DISTINCT src_id,dst_id,
                    $block_str block
             FROM archive_$measure
             WHERE new=1
             GROUP BY src_id,dst_id,block
            );";

    return $query;
}


sub get_latest_path_times {
    my ($block_str,$measure,$forecast_map,$forecast_model) = @_;

    my $query =
        "CREATE TEMPORARY TABLE
             a ( INDEX(src_id,dst_id) )
         ENGINE=MEMORY
         AS (SELECT MAX(s.time) last_time, s.src_id src_id, s.dst_id dst_id,
                    $block_str block
             FROM archive_$measure s, $forecast_map m, $forecast_model f
             WHERE s.id=m.archive_id
                   AND m.model_id=f.id
             GROUP BY s.src_id, s.dst_id, block
            );";

    return $query;
}


# Obtain path-blocks of count equal to $base.
# These are to have their first forecast generated.
sub get_path_base_counts {
    my ($block_str,$measure,$base) = @_;

    my $query =
        "CREATE TEMPORARY TABLE
             e ( INDEX(src_id,dst_id) )
         ENGINE=MEMORY
         AS (SELECT src_id,dst_id,
                    $block_str block,
                    count(*) tot
             FROM archive_$measure
             GROUP BY src_id,dst_id,block
             HAVING tot=$base
            );";

    return $query;
}


# Obtain path-blocks without new entries
sub get_edges_wo_new_entries {
    my ($block_str,$measure) = @_;

    my $query =
        "CREATE TEMPORARY TABLE
             d ( INDEX(src_id,dst_id) )
         ENGINE=MEMORY
         AS (SELECT distinct super.src_id,super.dst_id,super.block       # For edges without new entries, obtain...
             FROM (
                   SELECT DISTINCT src_id,dst_id,                        # all edges...
                          $block_str AS block
                   FROM archive_$measure
                   GROUP BY src_id,dst_id,block
                  ) super
             LEFT JOIN
                  (
                   SELECT src_id,dst_id,block                            # minus edges with new entries.
                   FROM n
                  ) sub
               ON (super.src_id=sub.src_id AND super.dst_id=sub.dst_id AND super.block=sub.block)
               WHERE sub.src_id IS NULL OR sub.dst_id IS NULL OR sub.block IS NULL
               GROUP BY super.src_id, super.dst_id, super.block
         );";

    return $query;
}


# Obtain path-blocks with at least one test time > it's recorded max time
sub get_edges_not_done {
    my ($block_str,$measure) = @_;

    my $query =
        "CREATE TEMPORARY TABLE
             g ( INDEX(src_id,dst_id) )
         ENGINE=MEMORY
         AS (
             SELECT st.src_id src_id, st.dst_id dst_id, count(*) tot, d.block
             FROM archive_$measure st, a, d
             WHERE st.src_id=a.src_id AND st.dst_id=a.dst_id
                 AND st.src_id=d.src_id AND st.dst_id=d.dst_id
                 AND a.block=d.block
                 AND d.block=$block_str
                 AND st.time>a.last_time
             GROUP BY st.src_id, st.dst_id, d.block
             HAVING tot>0
         );";

    return $query;
}


sub get_latest_history {
    my ($measure,$base,$cols,$block_str) = @_;

    my $data_items = $cols * $base;

    my $query =
        "SELECT t.src_id,t.dst_id,a.block,
                SUBSTRING_INDEX(GROUP_CONCAT(t.time,',',t.measure ORDER BY t.time desc), ',', $data_items) AS most_recent_time_measure_pairs
         FROM
            archive_$measure t, a, c, n
         WHERE t.src_id=a.src_id
               AND t.dst_id=a.dst_id

               AND t.src_id=c.src_id
               AND t.dst_id=c.dst_id

               AND t.src_id=n.src_id
               AND t.dst_id=n.dst_id

               AND c.block=n.block
               AND c.block=a.block
               AND $block_str=c.block

               AND t.time<=a.last_time

         GROUP BY
             t.src_id,t.dst_id,a.block;";

    return $query;
}


sub get_earliest_history {
    my ($measure,$forecast_model,$base,$cols,$block_str) = @_;

    my $data_items = $cols * $base;
    my $query =
        "SELECT
           e.src_id,e.dst_id,e.block,
           SUBSTRING_INDEX(GROUP_CONCAT(t.time,',',t.measure ORDER BY t.time desc), ',', $data_items) AS earliest_time_measure_pairs
         FROM
           archive_$measure t, e, n
         WHERE t.src_id=e.src_id AND t.dst_id=e.dst_id
             AND e.src_id=n.src_id AND e.dst_id=n.dst_id
             AND e.block=n.block
             AND e.block=$block_str
         GROUP BY
             e.src_id,e.dst_id,e.block

         UNION

         SELECT  # Paths previously missed. Note use of asc order
           t.src_id,t.dst_id,c.block,
           SUBSTRING_INDEX(GROUP_CONCAT(t.time,',',t.measure ORDER BY t.time asc), ',', $data_items) AS earliest_time_measure_pairs
         FROM
           archive_$measure t, c, v
           LEFT JOIN ($forecast_model f)
         ON (v.src_id=f.src_id AND v.dst_id=f.dst_id AND f.src_id IS NULL)
         WHERE t.src_id=c.src_id
               AND t.dst_id=c.dst_id
               AND c.src_id=v.src_id
               AND c.dst_id=v.dst_id
               AND c.block=v.block
               AND c.block=$block_str
               AND c.tot=v.nulltot
         GROUP BY
           t.src_id,t.dst_id,c.block;";

    return $query;
}


sub get_unmapped_edges {
    my $query =
        "SELECT c.src_id,c.dst_id,c.block
         FROM c,v
         WHERE c.src_id=v.src_id
               AND c.dst_id=v.dst_id
               AND c.block=v.block
               AND c.tot=v.nulltot;";

    return $query;
}


sub get_catchup_history {
    my ($measure,$base,$cols,$block_str) = @_;

    my $data_items = $cols * $base;
    my $query =
        "SELECT
           t.src_id,t.dst_id,a.block,
           SUBSTRING_INDEX(GROUP_CONCAT(t.time,',',t.measure ORDER BY t.time desc), ',', $data_items) AS catchup_time_measure_pairs
         FROM
           archive_$measure t,
           a,
           c,
           g
         WHERE
           t.src_id=a.src_id
           AND t.dst_id=a.dst_id

           AND t.src_id=c.src_id
           AND t.dst_id=c.dst_id

           AND t.src_id=g.src_id
           AND t.dst_id=g.dst_id

           AND a.block=c.block
           AND a.block=g.block
           AND a.block=$block_str

           AND t.time<=a.last_time
         GROUP BY
           t.src_id,t.dst_id,a.block;";

    return $query;
}


# ______________________________________________
#
# For the audit process

sub get_fcast {
    my ($forecast_model) = @_;

    my $query =
           "SELECT id, last_time
            FROM $forecast_model
            WHERE src_id=?
              AND dst_id=?
              AND grouping=?
              AND new=1
            ORDER BY id;";

    return $query;
}


sub insert_map {
    my ($forecast_map) = @_;

    return "INSERT IGNORE INTO $forecast_map
            (archive_id,model_id)
            VALUES (?,?);";
}


sub get_new_measure_records {
    my ($measure,$block_str) = @_;

    my $query =
           "SELECT id, time
            FROM archive_$measure
            WHERE src_id=?
              AND dst_id=?
              AND time>=?
              AND $block_str=?
            ORDER BY time;";

    return $query;
}


sub update_fcast {
    my ($forecast_model) = @_;

    return "UPDATE $forecast_model
            SET last_time=NULL,
                new=0
            WHERE id=?;";
}


# ______________________________________________
#
# For the load process

# esmond to staging table mapping
# ts                -> time
# measurement-agent -> mon_ip
# source            -> src_ip
# destination       -> dst_ip
# val               -> measure
# metadata-key      -> mkey
sub load_archive {
    my ($input, $archive) = @_;

    my $sql = qq[LOAD DATA LOCAL INFILE '$input'
      IGNORE INTO TABLE staging_$archive
      FIELDS TERMINATED BY ','
      ENCLOSED BY '"'
      ESCAPED BY '"'
      LINES TERMINATED BY '\n'
      (time,mon_ip,src_ip,dst_ip,measure,mkey);];

    return $sql;
}


# This deletion is used to prevent occasional records with an empty IP
# from being left in the staging table. Setting a trigger to change it
# to NULL if empty and relying on the NOT NULL constraint of the field
# works, but it does not ignore the error; insertion is halted despite
# the use of IGNORE with LOAD and IGNORE supposedly being able to work
# with ER_BAD_NULL_ERROR. (Maybe a v5.7 feature?)
sub delete_from_staging_archive {
    my ($archive) = @_;

    my $sql = "DELETE FROM staging_$archive WHERE src_ip='' OR dst_ip='' OR mon_ip='';";
    return $sql;
}


# ______________________________________________
#
# For the node naming process
# The check on node_name IS NULL picks up new, unnamed nodes.
# The check on node_name=node_ip picks up previously unnamed nodes that may now have a name.

sub get_unnamed_nodes {
    my $sql = "SELECT id,node_ip FROM nodes WHERE node_ip<>'' AND (node_name IS NULL OR node_name=node_ip OR node_name='reache');";

    return $sql;
}


sub insert_node_name {
    my $sql = "UPDATE nodes SET node_name=? WHERE id=?";
}

# ______________________________________________
#
# For the nodes insert process
# mon_ip is included because, although rare, a monitoring node may not be a source or destination.
sub get_nodes {
    my ($archive) = @_;

    my $sql = "SELECT DISTINCT src_ip node_ip FROM staging_$archive
               UNION
               SELECT DISTINCT dst_ip node_ip FROM staging_$archive
               UNION
               SELECT DISTINCT mon_ip node_ip FROM staging_$archive;";

    return $sql;
}


sub insert_nodes {
    my $sql = "INSERT IGNORE INTO nodes
               (node_ip,node_name,archive_id)
               VALUES (?,?,?);";

    return $sql;
}


# ______________________________________________
#
# For the update archive process

sub get_archive_nodes {
    my ($archive_id) = @_;

    my $sql = "SELECT id, node_ip FROM nodes WHERE archive_id = $archive_id;";

    return $sql;
}


sub get_new_staging_records {
    my ($archive) = @_;

    my $sql = "SELECT id,time,mon_ip,src_ip,dst_ip,measure,mkey FROM staging_$archive WHERE new=1;";

    return $sql;
}


sub insert_archive {
    my ($archive) = @_;

    my $sql =  "INSERT IGNORE INTO archive_$archive (id,time,mon_id,src_id,dst_id,measure,mkey,new) VALUES(?,?,?,?,?,?,?,1);";

    return $sql;
}


sub mark_staging_records {
    my ($archive) = @_;

    my $sql = "UPDATE staging_$archive SET new=0;";

    return $sql;
}


1;
