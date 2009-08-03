#!/usr/bin/perl -w

##############################################################################
# This scripts updates the box and ant tables
# - data count for each antenna and box
# - the time with the last dataset (if older then 1 Week, maybe defect)
#
# The script can be started with the '--all' arguments to count for all
# days in the '$TABLE_DATA' table,
# instead of just the days in the $DAYS_TO_COUNT_TABLE table,
# or/and with the '--quiet' argument for suppress script output.
#
# rleuthold@access.ch - 9.3.2009
##############################################################################
use strict;

use DBHandler;
use XMLPaths;
use DBTables;
use IO::File;
use Data::Dumper;
use Getopt::Long;

##############################################################################
##
# Global variables
my $DBH; 
my $TABLES		= DBTables->new();
my $PATHS		= XMLPaths->new();
my $PERLCONFIG 	= PerlConfig->new();

# Paths / directories
my $DATA_PATH 	= $PATHS->get_path('data');
my $IMPORTED_FOLDER 	= $PATHS->get_path('imported');

my $SCRIPT_PATH	= $PERLCONFIG->get_scriptsfolder();

# database tables
my $TABLE_RFIDS	= $TABLES->get_table_name('rfids');
my $TABLE_DIR   = $TABLES->get_table_name('direction_results');
my $TABLE_RES	= $TABLES->get_table_name('results');
my $TABLE_BOX 	= $TABLES->get_table_name('boxes');
my $TABLE_DATA	= $TABLES->get_table_name('data');
my $TABLE_ANT 	= $TABLES->get_table_name('antennas');
my $TABLE_RFIDS_COUNT	= $TABLES->get_table_name('rfid_count');
my $TABLE_BOXES_COUNT	= $TABLES->get_table_name('box_count');
my $TABLE_ANTS_COUNT	= $TABLES->get_table_name('antenna_count');

##
# The table containing the days to count. This table is created in the logimport.pl script
# and will be dropped and the end of this script.
my $DAYS_TO_COUNT_TABLE = $TABLES->get_days_to_count_table();

##
# The folder with the daily log files
my $DAY_FOLDER;

my @DAYS_TO_COUNT;
my $ALL_DAYS = 0;
my $QUIET = 0;

####################################
# PREAMBLE
####################################

##
# Getting command line options
GetOptions("quiet"=>\$QUIET, "all"=>\$ALL_DAYS);



if( $QUIET == 0 ) {

	print"\n================================================\n";
	print"STARTING COUNTER.PL";
	print"\n================================================\n";
}

#############################################################################
# open db connection
$DBH = DBHandler->new()->connect();
#############################################################################

## 
# check if we have days to count for. If not, we stop the script immediately.
if( ($DBH->tables('', '', $DAYS_TO_COUNT_TABLE, 'TABLE')) == 0) {
	if( $QUIET == 0 ) {
		print "no days to count - bye\n";
	} 
	$DBH->disconnect();
	exit;
}

################################
# set up results file

##
# where should we write the output 
if( $QUIET == 0 ) {
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime time;
	my $dayDate = $mday."_". ($mon+1) ."_". ($year+1900) ."/";
	$DAY_FOLDER = $IMPORTED_FOLDER . $dayDate;
	
	##
	# Try to create folders if they don't exist
	mkdir($IMPORTED_FOLDER, 0771) unless (-d $IMPORTED_FOLDER);
	mkdir($DAY_FOLDER, 0771) unless (-d $DAY_FOLDER);
	
	my $filename = $DAY_FOLDER. "counter_log\.txt";	# the filename with some kind of timestamp
	sysopen (RES, $filename, O_CREAT |O_WRONLY, 0755)  or die("Can't open result file '$filename': $!");
}

##
# PREPARE SQLS

##
# ANTS
my $COUNT_ANTS_DATA_DAILY = $DBH->prepare(qq{ SELECT ant, COUNT(id) AS data_count FROM $TABLE_DATA WHERE `time` BETWEEN ? AND (SELECT DATE_ADD( ? , INTERVAL 1 DAY)) GROUP BY ant })	
	or die("Could not prepare statement to select $TABLE_ANT counts in $TABLE_DATA: " . $DBH->errstr);
	
my $COUNT_ANTS_DIR_DAILY = $DBH->prepare(qq{ SELECT ant, LEFT(ant, 2) as box, COUNT(id) as dir_count FROM $TABLE_DATA WHERE `time` BETWEEN ? AND (SELECT DATE_ADD( ? , INTERVAL 1 DAY)) AND i != 1 GROUP BY ant })	
	or die("Could not prepare statement to select $TABLE_ANT counts in $TABLE_DIR: " . $DBH->errstr);
	
my $COUNT_ANTS_RES_DAILY = $DBH->prepare(qq{ SELECT ant, i, COUNT(id) as res_count FROM $TABLE_DATA WHERE `time` BETWEEN ? AND (SELECT DATE_ADD( ? , INTERVAL 1 DAY)) AND i IN(3,4) GROUP BY ant })	
	or die("Could not prepare statement to select $TABLE_ANT in $TABLE_DATA: " . $DBH->errstr);							
	
##
# INSERT DAILY ANTENNAS COUNT
my $DAILY_ANTS = $DBH->prepare(qq{ INSERT INTO $TABLE_ANTS_COUNT (day, id, data_count, dir_count, res_count) VALUES ( ?,?,?,?,? ) })	
	or die("Could not prepare statement to insert or update daily data in $TABLE_ANTS_COUNT: " . $DBH->errstr);
	
	
##
# BOXES
my $COUNT_BOXES_DATA_DAILY = $DBH->prepare(qq{ SELECT LEFT(ant,2) AS box, COUNT(id) AS data_count FROM $TABLE_DATA WHERE `time` BETWEEN ? AND (SELECT DATE_ADD( ? , INTERVAL 1 DAY)) GROUP BY box })	
	or die("Could not prepare statement to select $TABLE_BOX counts in $TABLE_DATA: " . $DBH->errstr);
	
my $COUNT_BOXES_DIR_DAILY = $DBH->prepare(qq{ SELECT box, COUNT(id) as dir_count FROM $TABLE_DIR WHERE `time` BETWEEN ? AND (SELECT DATE_ADD( ? , INTERVAL 1 DAY)) GROUP BY box })	
	or die("Could not prepare statement to select $TABLE_BOX counts in $TABLE_DIR: " . $DBH->errstr);
	
my $COUNT_BOXES_RES_DAILY = $DBH->prepare(qq{ SELECT box, COUNT(id) as res_count FROM $TABLE_RES WHERE `box_out` BETWEEN ? AND (SELECT DATE_ADD( ? , INTERVAL 1 DAY)) GROUP BY box })	
	or die("Could not prepare statement to select $TABLE_BOX in $TABLE_RES: " . $DBH->errstr);							
	
##
# INSERT DAILY BOXES COUNT
my $DAILY_BOXES = $DBH->prepare(qq{ INSERT INTO $TABLE_BOXES_COUNT (day, id, data_count, dir_count, res_count) VALUES ( ?,?,?,?,? ) })
	or die("Could not prepare statement to insert or update daily data in $TABLE_BOXES_COUNT: " . $DBH->errstr);
	
	
##
# RFIDS
my $COUNT_RFIDS_DATA_DAILY = $DBH->prepare(qq{ SELECT rfid, COUNT(id) AS data_count FROM $TABLE_DATA WHERE `time` BETWEEN ? AND (SELECT DATE_ADD( ? , INTERVAL 1 DAY)) GROUP BY rfid	})	
	or die("Could not prepare statement to select $TABLE_RFIDS counts in $TABLE_DATA: " . $DBH->errstr);
	
my $COUNT_RFIDS_DIR_DAILY = $DBH->prepare(qq{ SELECT rfid, COUNT(id) as dir_count FROM $TABLE_DIR WHERE `time` BETWEEN ? AND (SELECT DATE_ADD( ? , INTERVAL 1 DAY)) GROUP BY rfid })	
	or die("Could not prepare statement to select $TABLE_RFIDS counts in $TABLE_DIR: " . $DBH->errstr);
	
my $COUNT_RFIDS_RES_DAILY = $DBH->prepare(qq{ SELECT rfid, i, COUNT(id) as res_count FROM $TABLE_RES WHERE `box_out` BETWEEN ? AND (SELECT DATE_ADD( ? , INTERVAL 1 DAY)) GROUP BY rfid })	
	or die("Could not prepare statement to select $TABLE_RFIDS in $TABLE_RES: " . $DBH->errstr);							
	
	
##
# INSERT DAILY RFIDS COUNT 
my $DAILY_RFIDS = $DBH->prepare(qq{ INSERT INTO $TABLE_RFIDS_COUNT (day, id, data_count, dir_count, res_count) VALUES ( ?,?,?,?,? ) })	
	or die("Could not prepare statement to insert or update daily data in $TABLE_RFIDS_COUNT: " . $DBH->errstr);


##############################################################################
# MAIN
##############################################################################	
	
##
# Getting the days we have to count.
#

if( $ALL_DAYS == 1 ) {
	# If the script is started with the --all argument we count for all days in the database.
	@DAYS_TO_COUNT = @{$DBH->selectcol_arrayref("SELECT DISTINCT DATE(`time`) FROM $TABLE_DATA ORDER BY `time` ASC")}
		or die("Could not execute statement to get days in $TABLE_DATA: " . $DBH->errstr);
} else {
	# If the script is started with no argument we count for the days in the $DAYS_TO_COUNT_TABLE.
	@DAYS_TO_COUNT = @{$DBH->selectcol_arrayref("SELECT day FROM $DAYS_TO_COUNT_TABLE")}
		or die("Could not execute statement to get days in $DAYS_TO_COUNT_TABLE: " . $DBH->errstr);
}

if( $QUIET == 0 ) {
	my $feedback = "\n................................................\n";
	$feedback .= "Counting for the following dates:\n\n\t" . join("\n\t",@DAYS_TO_COUNT) . "\n";
	$feedback .= "\n\t[TOTAL " . scalar(@DAYS_TO_COUNT) . " days]";
	$feedback .= "\n................................................\n\n";
	
	print $feedback;
	printf RES $feedback;
	
}

##
# MAIN LOOP OVER DAYS
my $days_counted = 1;
my $days_to_count = @DAYS_TO_COUNT;
my $DAY_START_TIME;

foreach my $DAY (@DAYS_TO_COUNT) {
	
	##
	# This is a bit of a hack to avoid the use of the DATE() function in the SQL-statements.
	# The use of the function prevents MySQL from using the indices which makes the queries much slower.
	# (Performance is almost 10 times better) 
	$DAY_START_TIME = "$DAY 00:00:00"; 
	
	if( $QUIET == 0 ) {
		print "[ day $days_counted of $days_to_count ] $DAY ----------------------\n";		
		printf RES "[ day $days_counted of $days_to_count ] $DAY ----------------------\n";			
	}	
	
	##
	# Counting boxes data
	$DBH->do("DELETE FROM $TABLE_BOXES_COUNT WHERE day='$DAY'")
		or die("Could not execute statement to clear data from $TABLE_BOXES_COUNT for day $DAY: " . $DBH->errstr);			
		
	my $box_data = &CountBoxes($DAY_START_TIME);
	my $boxes_this_day = &insertDailyCount($box_data, $DAY, $DAILY_BOXES);
	
	if( $QUIET == 0 ) {
		print "[OK] boxes: $boxes_this_day\n";
		printf RES "[OK] boxes: $boxes_this_day\n";	
	}
	
	##
	# Counting antenna data
	$DBH->do("DELETE FROM $TABLE_ANTS_COUNT WHERE day='$DAY'")
		or die("Could not execute statement clear data from $TABLE_ANTS_COUNT for day $DAY: " . $DBH->errstr);
		
	my $ant_data = &CountAnts($DAY_START_TIME);
	my $ants_this_day = &insertDailyCount($ant_data, $DAY, $DAILY_ANTS);
	
	if( $QUIET == 0 ) {
		print "[OK] ants: $ants_this_day\n";
		printf RES "[OK] ants: $ants_this_day\n";	
	}
	
	##
	# Counting rfid data
	$DBH->do("DELETE FROM $TABLE_RFIDS_COUNT WHERE day='$DAY'")
		or die("Could not execute statement to clear data from $TABLE_RFIDS_COUNT for day $DAY: " . $DBH->errstr);	
		
	my $rfid_data = &CountRfids($DAY_START_TIME);
	my $rfids_this_day = &insertDailyCount($rfid_data, $DAY, $DAILY_RFIDS);
	
	if( $QUIET == 0 ) {
		print "[OK] rfids: $rfids_this_day\n";
		printf RES "[OK] rfids: $rfids_this_day\n";	
	}
	
	
	
	##
	#uncomment to test one day
	# if($days_counted == 3) {
	# last;	
	#}
	if( $QUIET == 0 ) {
		my $feedback = "------------------------------------------------\n\n";
		print $feedback;
		printf RES $feedback; 
				
	}
	$days_counted++;
	
}

if( $QUIET == 0 ) {
	my $feedback = "\n------------------------------------------------\n";
	$feedback .= "Finishing up\n";
	$feedback .= "------------------------------------------------\n\n";
	
	print $feedback;
	printf RES $feedback;
	
}

##
# Updating rfid, box, ant table (data_count, dir_count, res_count, last_data)
##
# RFIDS
if( $QUIET == 0 ) {
	print "updating table $TABLE_RFIDS ... ";
	printf RES "updating table $TABLE_RFIDS ... ";
}

# Getting the rfids summary information

# I had to make two queries due to performance
my $LASTDATA_RFIDS = $DBH->selectall_hashref(qq{ SELECT rfid, MAX(time) AS last_data FROM $TABLE_DATA GROUP BY rfid }, 'rfid') 	
	|| die("Could not execute statement to get last_data for rfids from $TABLE_DATA: " . $DBH->errstr);

my $DATA_RFID = $DBH->selectall_hashref(qq{ SELECT DISTINCT id, SUM(data_count) AS data_count, SUM(dir_count) AS dir_count, SUM(res_count) AS res_count FROM $TABLE_RFIDS_COUNT GROUP BY id }, 'id')
	|| die("Could not execute the statement to collect the rfid summary information $TABLE_RFIDS: " . $DBH->errstr);	

# Bringing the information together
foreach my $rfid (keys %$DATA_RFID) {
	$DATA_RFID->{$rfid}->{'last_data'} = $LASTDATA_RFIDS->{$rfid}->{'last_data'};
}

my $UPDATE_RFID = $DBH->prepare(qq{ INSERT INTO $TABLE_RFIDS (id, data_count, dir_count, res_count, last) VALUES ( ?,?,?,?,? )
	ON DUPLICATE KEY UPDATE data_count = ?, dir_count= ?, res_count= ?, last= ?})	
	|| die("Could not prepare statement to update the tables in $TABLE_RFIDS: " . $DBH->errstr);
	
	
my $rfids = &updateTable($DATA_RFID, $UPDATE_RFID);
if( $QUIET == 0 ) {
	print "[OK] rfids: $rfids\n";
	printf RES "[OK] rfids: $rfids\n";
}
##
# ANTENNAS
if( $QUIET == 0 ) {
	print "updating table $TABLE_ANT ... ";
	printf RES "updating table $TABLE_ANT ... ";
}

# Getting the antennas summary information

# I had to make two queries due to performance
my $LASTDATA_ANTS = $DBH->selectall_hashref(qq{ SELECT ant, MAX(time) as last_data FROM $TABLE_DATA GROUP BY ant }, 'ant') 	
	|| die("Could not execute statement to get last_data for ants from $TABLE_DATA: " . $DBH->errstr);

my $DATA_ANT = $DBH->selectall_hashref(qq{ SELECT DISTINCT id, SUM(data_count) AS data_count, SUM(dir_count) AS dir_count, SUM(res_count) AS res_count FROM $TABLE_ANTS_COUNT GROUP BY id }, 'id')
	|| die("Could not execute the statement to collect the rfid summary information $TABLE_RFIDS: " . $DBH->errstr);	

# Bringing the information together
foreach my $ant (keys %$DATA_ANT) {
	$DATA_ANT->{$ant}->{'last_data'} = $LASTDATA_ANTS->{$ant}->{'last_data'};
}


my $UPDATE_ANT = $DBH->prepare(qq{ INSERT INTO $TABLE_ANT (id, data_count, dir_count, res_count, last) VALUES ( ?,?,?,?,? )
	ON DUPLICATE KEY UPDATE data_count = ?, dir_count = ?, res_count = ?, last = ?})	
	|| die("Could not prepare statement to update the tables in $TABLE_ANT: " . $DBH->errstr);

my $ants = &updateTable($DATA_ANT, $UPDATE_ANT);

if( $QUIET == 0 ) {
	print "[OK] antennas: $ants\n";
	printf RES "[OK] antennas: $ants\n";
}
##
# BOXES
if( $QUIET == 0 ) {
	print "updating table $TABLE_BOX ... ";	
	printf RES "updating table $TABLE_BOX ... ";	
}

# Getting the boxes summary information

# I had to make two queries due to performance
my $LASTDATA_BOXES = $DBH->selectall_hashref(qq{ SELECT LEFT(ant,2) AS box, MAX(time) as last_data FROM $TABLE_DATA GROUP BY box }, 'box') 	
	|| die("Could not execute statement to get last_data for boxes from $TABLE_DATA: " . $DBH->errstr);

my $DATA_BOX = $DBH->selectall_hashref(qq{ SELECT DISTINCT id, SUM(data_count) AS data_count, SUM(dir_count) AS dir_count, SUM(res_count) AS res_count FROM $TABLE_BOXES_COUNT GROUP BY id }, 'id')
	|| die("Could not execute the statement to collect the rfid summary information $TABLE_RFIDS: " . $DBH->errstr);	

# Bringing the information together
foreach my $box (keys %$DATA_BOX) {
	$DATA_BOX->{$box}->{'last_data'} = $LASTDATA_BOXES->{$box}->{'last_data'};
}
	
my $UPDATE_BOX = $DBH->prepare(qq{ INSERT INTO $TABLE_BOX (id, data_count, dir_count, res_count, last) VALUES ( ?,?,?,?,? )
	ON DUPLICATE KEY UPDATE data_count = ?, dir_count = ?, res_count = ?, last = ?})	
	|| die("Could not prepare statement to update $TABLE_BOX: " . $DBH->errstr);

my $boxes = &updateTable($DATA_BOX, $UPDATE_BOX);
if( $QUIET == 0 ) {
	print "[OK] boxes: $boxes\n";
	printf RES "[OK] boxes: $boxes\n";
}

####################################
# FINISH
####################################

##
# deleting the temporary table $DAYS_TO_COUNT_TABLE
#$DBH->do(qq{ DROP TABLE IF EXISTS $DAYS_TO_COUNT_TABLE }) || die("Could not drop table $DAYS_TO_COUNT_TABLE " . $DBH->errstr);

if( $QUIET == 0 ) {
	print"\n================================================\n";
	print "COUNTER.PL COMPLETE";
	print"\n================================================\n";
}
$DBH->disconnect();

# my @args = ( $SCRIPT_PATH."dbsync.pl");
# system(@args) == 0
# 	or die "system @args failed: $?";
exit;

##############################################################################
# SUBS
##############################################################################

# COUNT ANTENNA DATA
##############################################################################	
sub CountAnts {

	my $day = shift;
	
	if( $QUIET == 0 ) {
		print "=> calculating antenna count ... ";
	}
	
	
	##
	# hash for ant counts
	my $ant_counts = {};
	
	##
	# DATA COUNT ANT
	
	$COUNT_ANTS_DATA_DAILY->execute($day, $day)
		or die("Could not execute statement to count $TABLE_ANT in $TABLE_DATA: " . $DBH->errstr);	
		
	my $ants_data_count = $COUNT_ANTS_DATA_DAILY->fetchall_hashref('ant');
	
	# data count to hash
	foreach my $ant (keys %$ants_data_count) {
		my $data_count = $ants_data_count->{$ant}->{'data_count'} || 0;
		$ant_counts->{$ant}->{'data_count'} = $data_count;

	}
	
		
	##
	# DIR COUNT ANT
	$COUNT_ANTS_DIR_DAILY->execute($day, $day)
		or die("Could not execute statement to count $TABLE_ANT in $TABLE_DIR: " . $DBH->errstr);	
		
	my $ants_dir_count = $COUNT_ANTS_DIR_DAILY->fetchall_hashref('ant');
	
	# dir count to hash
	foreach my $ant (keys %$ants_dir_count) {
		my $dir_count = $ants_dir_count->{$ant}->{'dir_count'} || 0;
		$ant_counts->{$ant}->{'dir_count'} = $dir_count;	
	}
		
	##
	# RES COUNT ANT
	$COUNT_ANTS_RES_DAILY->execute($day, $day)
		or die("Could not execute statement to count $TABLE_ANT in $TABLE_RES: " . $DBH->errstr);	
	
	my $ants_res_count = $COUNT_ANTS_RES_DAILY->fetchall_hashref('ant');
	
	# res count to hash
	foreach my $ant (keys %$ants_res_count) {
		my $res_count = $ants_res_count->{$ant}->{'res_count'} || 0;
		$ant_counts->{$ant}->{'res_count'} = $res_count;
	}
	
	return $ant_counts;

}

# COUNT BOX DATA
##############################################################################	

sub CountBoxes {

	my $day = shift;
	
	if( $QUIET == 0 ) {
		print "=> calculating box count ... ";
	}
	
	
	##
	# hash for box counts
	my $boxes_counts = {};
	
	##
	# DATA COUNT BOX
	$COUNT_BOXES_DATA_DAILY->execute($day, $day)
		or die("Could not execute statement to count $TABLE_BOX in $TABLE_DATA: " . $DBH->errstr);	
		
	my $boxes_data_count = $COUNT_BOXES_DATA_DAILY->fetchall_hashref('box');
	
	# data count to hash
	foreach my $box (keys %$boxes_data_count) {
		my $data_count = $boxes_data_count->{$box}->{'data_count'} || 0;
		$boxes_counts->{$box}->{'data_count'} = $data_count;
	}
	
		
	##
	# DIR COUNT BOX
	$COUNT_BOXES_DIR_DAILY->execute($day, $day)
		or die("Could not execute statement to count $TABLE_BOX in $TABLE_DIR: " . $DBH->errstr);	
		
	my $boxes_dir_count = $COUNT_BOXES_DIR_DAILY->fetchall_hashref('box');
	
	# dir count to hash
	foreach my $box (keys %$boxes_dir_count) {
		my $dir_count 	= $boxes_dir_count->{$box}->{'dir_count'} || 0;
		$boxes_counts->{$box}->{'dir_count'} = $dir_count;	
	}
		
	##
	# RES COUNT BOX
	$COUNT_BOXES_RES_DAILY->execute($day, $day)
		or die("Could not execute statement to count $TABLE_BOX in $TABLE_RES: " . $DBH->errstr);	
	
	my $boxes_res_count = $COUNT_BOXES_RES_DAILY->fetchall_hashref('box');
	
	# res count to hash
	foreach my $box (keys %$boxes_res_count) {
	
		my $res_count = $boxes_res_count->{$box}->{'res_count'} || 0;
		$boxes_counts->{$box}->{'res_count'} = $res_count;
	}
	
	return $boxes_counts;

}



# COUNT RFID DATA
##############################################################################	

sub CountRfids {

	my $day = shift;
	
	if( $QUIET == 0 ) {
		print "=> calculating rfid count ... ";
	}
	
	
	##
	# hash for rfid counts
	my $rfids_counts = {};
	
	##
	# DATA COUNT RFID
	$COUNT_RFIDS_DATA_DAILY->execute($day, $day)
		or die("Could not execute statement to count $TABLE_RFIDS in $TABLE_DATA: " . $DBH->errstr);	
		
	my $rfids_data_count = $COUNT_RFIDS_DATA_DAILY->fetchall_hashref('rfid');
	
	# data count to hash
	foreach my $rfid (keys %$rfids_data_count) {
		my $data_count = $rfids_data_count->{$rfid}->{'data_count'} || 0;
		$rfids_counts->{$rfid}->{'data_count'} = $data_count;
	}
	
		
	##
	# DIR COUNT RFID
	$COUNT_RFIDS_DIR_DAILY->execute($day, $day)
		or die("Could not execute statement to count $TABLE_RFIDS in $TABLE_DIR: " . $DBH->errstr);	
		
	my $rfids_dir_count = $COUNT_RFIDS_DIR_DAILY->fetchall_hashref('rfid');
	
	# dir count to hash
	foreach my $rfid (keys %$rfids_dir_count) {
		my $dir_count = $rfids_dir_count->{$rfid}->{'dir_count'} || 0;
		$rfids_counts->{$rfid}->{'dir_count'} = $dir_count;	
	}
		
	##
	# RES COUNT RFID
	$COUNT_RFIDS_RES_DAILY->execute($day, $day)
		or die("Could not execute statement to count $TABLE_RFIDS in $TABLE_RES: " . $DBH->errstr);	
	
	my $rfids_res_count = $COUNT_RFIDS_RES_DAILY->fetchall_hashref('rfid');
	
	# res count to hash
	foreach my $rfid (keys %$rfids_res_count) {
	
		my $res_count = $rfids_res_count->{$rfid}->{'res_count'} || 0;
		$rfids_counts->{$rfid}->{'res_count'} = $res_count;
	}
	
	return $rfids_counts;

}


##
# insert daily data count into the db
sub insertDailyCount {

	my ($data, $day, $sth) = @_;
	
	foreach my $item (keys %$data) {
		my $data_count  = $data->{$item}->{'data_count'} ||0;
		my $dir_count  = $data->{$item}->{'dir_count'} ||0;
		my $res_count  = $data->{$item}->{'res_count'} || 0;
		
		
		$sth->execute($day, $item, $data_count , $dir_count, $res_count)
			or die("Could not execute statement to insert daily data into data count table: " . $DBH->errstr);
		
	}
	
	return keys %$data;

}

##
# update main tables for rfid boxes antennas
sub updateTable {

	my ($data,$sth) = @_;
	
	foreach my $id (keys %$data) {
	
		my $data_count 	= $data->{$id}->{'data_count'}	|| 0;
		my $dir_count 	= $data->{$id}->{'dir_count'} 	|| 0;
		my $res_count 	= $data->{$id}->{'res_count'} 	|| 0;
		my $last 		= $data->{$id}->{'last_data'} 	|| '0000-00-00 00:00:00';
		
		$sth->bind_param(1, $id);
		$sth->bind_param(2, $data_count, SQL_INTEGER);
		$sth->bind_param(3, $dir_count, SQL_INTEGER);
		$sth->bind_param(4, $res_count, SQL_INTEGER);
		$sth->bind_param(5, $last,SQL_DATETIME);
		$sth->bind_param(6, $data_count, SQL_INTEGER);
		$sth->bind_param(7, $dir_count, SQL_INTEGER);
		$sth->bind_param(8, $res_count, SQL_INTEGER);
		$sth->bind_param(9, $last, SQL_DATETIME);
		
	
		$sth->execute($id, $data_count, $dir_count, $res_count, $last, $data_count,$dir_count,$res_count, $last)
			or die("Could not execute statement to insert or update table : " . $DBH->errstr);		
			
	}
	
	return keys %$data;

}

