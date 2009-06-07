#!/usr/bin/perl -w

###############################################################           
# Resetting the database so that the data can be recalculated #
# starting with the 'searchres.pl'		                      #
#															  #
# rleuthold@access.ch - 8.1.2009							  #
###############################################################

use strict;
use DBI;
use Cwd;
use Fcntl;
use Data::Dumper;

use lib 'lib';
use lib::DBHandler;
use lib::DBTables;

##############################
# Global variables

my $DBH; 
my $TABLES		= DBTables->new();

my $TABLE_RFIDS			= $TABLES->get_table_name('rfids');
my $TABLE_DATA			= $TABLES->get_table_name('data');
my $TABLE_DIR			= $TABLES->get_table_name('direction_results');
my $TABLE_RES			= $TABLES->get_table_name('results');
my $TABLE_MEETINGS		= $TABLES->get_table_name('meetings');
my $TABLE_RFIDS_COUNT	= $TABLES->get_table_name('rfid_count');
my $TABLE_ANT_COUNT		= $TABLES->get_table_name('antenna_count');
my $TABLE_BOX_COUNT		= $TABLES->get_table_name('box_count');

####################################
# PREAMBLE
####################################

print qq{
###############################################################           
# Resetting the database so that the data can be recalculated #
# starting with the 'searchres.pl' script.                    #
############################################################### 
};

####################################
# open db connection
$DBH = DBHandler->new()->connect();

##############################################################	           
# MAIN
##############################################################	 

###
# resetting the data table
print"\t=> Updating $TABLE_DATA table ...\n";

# Reset the data i=4 which are the 'IN' part of a data result (3-1) -> (1) to i=2
$DBH->do(qq{UPDATE $TABLE_DATA, $TABLE_DIR SET $TABLE_DATA.i = 2, $TABLE_DATA.res_id = null WHERE $TABLE_DIR.innerdataid = $TABLE_DATA.id AND $TABLE_DIR.i = 4;}) 
	or die("Could not update $TABLE_DATA: " . $DBH->errstr);
	
$DBH->do(qq{UPDATE $TABLE_DATA, $TABLE_DIR SET $TABLE_DATA.i = 2, $TABLE_DATA.res_id = null WHERE $TABLE_DIR.outerdataid = $TABLE_DATA.id AND $TABLE_DIR.i = 4;}) 
	or die("Could not update $TABLE_DATA: " . $DBH->errstr);
	
# The rest of the i=4 are the ones used as OUT part in the data results (3-1) -> (1).
$DBH->do(qq{UPDATE $TABLE_DATA SET i=1, res_id = null WHERE i=4}) 
	or die("Could not update $TABLE_DATA: " . $DBH->errstr);

# Reset the data which is a part of a direction result 
$DBH->do(qq{UPDATE $TABLE_DATA SET i=2, res_id = null WHERE i=3}) 
	or die("Could not update $TABLE_DATA: " . $DBH->errstr);
	
### 
# resetting the dir table
print"\t=> Updating $TABLE_DIR table...\n";
$DBH->do(qq{UPDATE $TABLE_DIR SET i=0}) 
	or die("Could not update $TABLE_DIR: " . $DBH->errstr);	

### 
# resetting the res table
print "\t=> Truncating $TABLE_RES table ...\n";
$DBH->do(qq{TRUNCATE $TABLE_RES}) 
	or die("Could not truncate $TABLE_RES: " . $DBH->errstr);

### 
# resetting meetings 
print "\t=> Truncating $TABLE_MEETINGS table ...\n";
$DBH->do(qq{TRUNCATE $TABLE_MEETINGS}) 
	or die("Could not truncate $TABLE_MEETINGS: " . $DBH->errstr);

### 
# resetting the rfids table
print"\t=> Updating rfid table ...\n";
$DBH->do(qq{UPDATE $TABLE_RFIDS SET i=2, res='0000-00-00 00:00:00' WHERE i=3}) 
	or die("Could not update $TABLE_RFIDS: " . $DBH->errstr);		
	
### 
# resetting rfid counts
print"\t=> Truncating $TABLE_RFIDS_COUNT ...\n";
$DBH->do(qq{TRUNCATE $TABLE_RFIDS_COUNT}) 
	or die("Could not truncate $TABLE_RFIDS_COUNT: " . $DBH->errstr);
	
### 
# resetting ant counts
print"\t=> Truncating $TABLE_ANT_COUNT...\n";
$DBH->do(qq{TRUNCATE $TABLE_ANT_COUNT}) 
	or die("Could not truncate $TABLE_ANT_COUNT: " . $DBH->errstr);
	
### 
# resetting box counts
print"\t=> Truncating $TABLE_BOX_COUNT...\n";
$DBH->do(qq{TRUNCATE $TABLE_BOX_COUNT}) 
	or die("Could not delete from $TABLE_BOX_COUNT: " . $DBH->errstr);		


###
# update the status for the rfid in the rfids table
# uncomment next line to test only one rfid
#exit;


#########################################################
# END
#########################################################;

# close db and result File
$DBH->disconnect();

print qq {
********	           
* DONE *
********	 
};
