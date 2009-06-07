#!/usr/bin/perl -w

###############################################################           
# Resetting the database so that the meeting data can be      #
# recalculated starting with the 'counter.pl' import script.  #
#															  #
# rleuthold@access.ch - 10.1.2009							  #
###############################################################
use strict;

use lib 'lib';
use lib::DBHandler;
use lib::XMLPaths;
use lib::DBTables;

use Data::Dumper;


####################################
# PREAMBLE
####################################

print qq {
###############################################################           
# Resetting the database so that the meeting data can be      #
# recalculated starting with the 'counter.pl' import script.  #
###############################################################
};

##############################################################################
##
# Global variables
my $DBH; 
my $TABLES		= DBTables->new();

# database tables
my $TABLE_RFIDS	= $TABLES->get_table_name('rfids');
my $TABLE_BOX 	= $TABLES->get_table_name('boxes');
my $TABLE_ANT 	= $TABLES->get_table_name('antennas');
my $TABLE_RFIDS_COUNT = $TABLES->get_table_name('rfid_count');
my $TABLE_BOX_COUNT	= $TABLES->get_table_name('box_count');
my $TABLE_ANT_COUNT	= $TABLES->get_table_name('antenna_count');


#############################################################################
# open db connection
$DBH = DBHandler->new()->connect();
#############################################################################

##############################################################	           
# MAIN
##############################################################	 

##
# Resetting count tables
foreach my $count_table (@{[$TABLE_RFIDS_COUNT, $TABLE_BOX_COUNT, $TABLE_ANT_COUNT]}) {
	print "=> resetting table '$count_table' ... ";
	$DBH->do( "TRUNCATE $count_table" )
		or die("Could not execute statement to reset table '$count_table': " . $DBH->errstr);	
	print "[OK]\n";
}

print"\n";

##
# Updating counts in other tables
foreach my $data_table (@{[$TABLE_RFIDS, $TABLE_BOX, $TABLE_ANT]}) {
	print "=> updating counts in table '$data_table' ... ";
	$DBH->do( "UPDATE $data_table SET data_count = 0, dir_count = 0, res_count = 0, last = '0000-00-00 00:00:00'" )
		or die("Could not execute statement to update table '$data_table': " . $DBH->errstr);	
	print "[OK]\n";
}
	

#########################################################
# END
#########################################################;

# close db and result File
$DBH->disconnect();

print q {
********	           
* DONE *
********	 
};