#!/usr/bin/perl -w

###############################################################           
# Resetting the database so that the data can be recalculated #
# starting with the 'searchdir.pl'		                      #
#															  #
# rleuthold@access.ch - 8.1.2009							  #
###############################################################

use strict;
use DBI;
use Cwd;
use Fcntl;

use lib 'lib';
use Data::Dumper;
use lib::DBHandler;
use lib::DBTables;

my $DBH; 
my $TABLES		= DBTables->new();

my $TABLE_RFIDS	= $TABLES->get_table_name('rfids');
my $TABLE_DATA	= $TABLES->get_table_name('data');
my $TABLE_DIR	= $TABLES->get_table_name('direction_results');
my $TABLE_RES	= $TABLES->get_table_name('results');
my $TABLE_MEETINGS	= $TABLES->get_table_name('meetings');
my $DAYS_TO_COUNT_TABLE = $TABLES->get_days_to_count_table();

print qq {
###############################################################           
# Resetting the database so that the data can be recalculated #
# starting with the 'searchdir.pl' script.                    #
############################################################### 
};

####################################
# PREAMBLE
####################################

####################################
# open db connection
$DBH = DBHandler->new()->connect();

##############################################################	           
# MAIN
##############################################################	 

##
# clear the dir table data
print "\t=> Truncating $TABLE_DIR table ...\n";
my $clearDirResSQL = qq{TRUNCATE `$TABLE_DIR`};	
$DBH->do($clearDirResSQL) or die("Could not delete $TABLE_DIR: " . $DBH->errstr);

##
# reset the data table
print "\t=> Updating $TABLE_DATA table ...\n";
my $resetDataSQL = qq{UPDATE $TABLE_DATA SET i=0, dir_id = null, res_id = null WHERE i != 0};
$DBH->do($resetDataSQL) or die("Could not update $TABLE_DATA: " . $DBH->errstr);

## 
# updating the records (i, dir) in the rfid table
print "\t=> Updating $TABLE_RFIDS table ...\n";
my $updateSQL = qq{UPDATE $TABLE_RFIDS SET i=0, dir='0000-00-00 00:00:00'};
$DBH->do($updateSQL) or die("Could not update $TABLE_RFIDS: " . $DBH->errstr);

## 
# truncating results table
print "\t=> Truncating $TABLE_RES table ...\n";
my $truncateResSQL = qq{TRUNCATE $TABLE_RES};
$DBH->do($truncateResSQL) or die("Could not truncate $TABLE_RES: " . $DBH->errstr);

## 
# truncating meetings table
print "\t=> Truncating $TABLE_MEETINGS table ...\n";
my $truncateMeetingsSQL = qq{TRUNCATE $TABLE_MEETINGS};
$DBH->do($truncateMeetingsSQL) or die("Could not truncate $TABLE_MEETINGS: " . $DBH->errstr);

##
# update the status for the rfid in the rfids table
# uncomment next line to test only one rfid
#exit;


#########################################################
# Ending
#########################################################

# close db and result File
$DBH->disconnect();

print qq {
********	           
* DONE *
********	 
};