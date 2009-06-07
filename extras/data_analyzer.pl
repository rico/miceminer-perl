#!/usr/bin/perl -w


##############################################################################
# This scripts does the data import for the log files with real timestamps
# - reads out the logfile an puts the data in the 'data' table
#
# rleuthold@access.ch // 20.2.2008
##############################################################################

use strict;
use DBI;
use POSIX;
use Fcntl;
use Data::Dumper;
use Date::Calc qw(:all);
use File::stat;
use IO::File;


# VARIABLES
my @ARGS;
my @STARTTIME;
my @ENDTIME;
my $RESULTFILE;
my $SERIES;

# counters
my $DATACOUNT	= 0;




##############################################################################
# SET THIS !!!!!!!!!!!!!
##############################################################################

# my $TABLE_DATA	= "data";
# my $TABLE_LOGS	= "logs";
# my $TABLE_RFIDS	= "rfid";
# my $BASE_PATH 	= "/var/www/mouse/";
# my $DATA_PATH 	= $BASE_PATH. "data/";
# my $ANALYSIS_PATH 	= $DATA_PATH. "analysis/";
# my $SCRIPT_PATH	= $BASE_PATH. "perl/"; 
# my $SUBFOLDER 	= "imported/"; # Where you want the imported files moved to
# my $DATEFORMAT	= "20%02d-%02d-%02d %02d:%02d:%02d";

my $TABLE_DATA	= "data";
my $TABLE_LOGS	= "logs";
my $TABLE_RES	= "res";
my $TABLE_RFIDS	= "rfid";
my $BASE_PATH 	= "/Library/WebServer/mice/";
my $DATA_PATH 	= $BASE_PATH. "data/";
my $ANALYSIS_PATH 	= $DATA_PATH. "analysis/";
my $SCRIPT_PATH	= $BASE_PATH. "perl/"; 
my $SUBFOLDER 	= "imported/"; # Where you want the imported files moved to
my $DATEFORMAT	= "20%02d-%02d-%02d %02d:%02d:%02d";

my $DAY_FOLDER;
my @RFIDS;
my @DAYS_IN_DB;

##############################################################################

#############################################################################
# open db connection
my $DBH = DBI->connect('DBI:mysql:micedata;mysql_socket=/var/mysql/mysql.sock', 'importer', 'wasser',
			{ RaiseError => 0, AutoCommit => 0 })
		or die ("Could not connect to database: $DBI::errstr");

# my $DBH = DBI->connect('DBI:mysql:micedata_test;mysql_read_default_file=../conf/dblogin.conf',undef, undef,
# 					{ RaiseError => 0, AutoCommit => 0 }) 
# 				or die "Could not connect to database: $DBI::errstr";
#############################################################################

##############################################################	           
# MAIN
##############################################################	 

##
# creating day subfolder
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime time;
my $dayDate = $mday."_". ($mon+1) ."_". ($year+1900) ."/";
$DAY_FOLDER = $ANALYSIS_PATH . $dayDate;
mkdir($DAY_FOLDER, 0771) unless (-d $DAY_FOLDER);		

##
# getting rfids
@RFIDS = map {$_->[0]} @{$DBH->selectall_arrayref("SELECT id FROM $TABLE_RFIDS")};

##
# getting days
@DAYS_IN_DB = map {$_->[0]} @{$DBH->selectall_arrayref("SELECT DISTINCT DATE(time) FROM $TABLE_DATA ORDER BY time")};
		

foreach my $DAY (@DAYS_IN_DB) {

	# my $which_antennas_file = "$ANALYSIS_PATH$dayDate". "rfid_ant_per_day.txt";
# 	open(ANT, "+> $which_antennas_file") or die("Can't open file $which_antennas_file: $!");
# 	&WhichAntennas($DAY);
# 	close(ANT);
	
	my $duration_val_file = "$ANALYSIS_PATH$dayDate/rfid_box_duration.txt";
	open(DUR, "+> $duration_val_file") or die("Can't open file $duration_val_file: $!");
	&DurationVal($DAY);
	close(DUR);
	
	
	##
	# uncomment to test one day
	#exit;
}

##############################################################	           
# SUBS
##############################################################	 

sub WhichAntennas {

	my $day = shift;
	
	my $get_ants_sql = $DBH->prepare("SELECT DISTINCT ant from $TABLE_DATA WHERE RIGHT(ant,1) =  '3' AND rfid = ? AND DATE(time) = ?");
	
	foreach my $rfid (@RFIDS) {
	
		$get_ants_sql->execute($rfid, $day)
			or die("Could not execute statement for ant: " . $DBH->errstr);
		
		print ANT "$day\t$rfid";
		
		my $count = 0;
		my $ants_string = '';
		while (my $rec = $get_ants_sql->fetchrow_arrayref()) { 
			$ants_string .= "\t" . @$rec[0];
			$count++;

		}
		
		print ANT "\t$count";
		print ANT $ants_string . "\n";
		

	}
	
	#exit;
	

}

sub DurationVal {

	my $day = shift;
	
	my $get_ants_sql = $DBH->prepare("SELECT DISTINCT box from $TABLE_RES WHERE dt > '00:00:06' AND DATE(box_in) = ?  AND DATE(box_in) = ? AND rfid = ?");
	
	foreach my $rfid (@RFIDS) {
	
		$get_ants_sql->execute($day, $day, $rfid)
			or die("Could not execute statement for ant: " . $DBH->errstr);
		
		print DUR "$day\t$rfid";
		
		my $count = 0;
		my $boxes_string = '';
		while (my $rec = $get_ants_sql->fetchrow_arrayref()) { 
			$boxes_string .= "\t" . @$rec[0];
			$count++;

		}
		
		print DUR "\t$count";
		print DUR $boxes_string . "\n";

	}

}