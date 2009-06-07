#!/usr/bin/perl

###############################################################           
# Resetting the database so that the meeting data can be      #
# recalculated starting with the 'meetings.pl' import script. #
#															  #
# rleuthold@access.ch - 10.1.2009							  #
###############################################################
use strict;
use warnings;

use Data::Dumper;
use Date::Calc qw(:all);

use lib 'lib';
use lib::DBHandler;
use lib::XMLPaths;
use lib::DBTables;

##############################################################################
##
# Global variables
my $DBH;
my $TABLES		= DBTables->new();
my $PATHS		= XMLPaths->new();
my $PERLCONFIG	= PerlConfig->new();

# Paths / directories
my $SCRIPT_PATH	= $PERLCONFIG->get_scriptsfolder();

# database tables
my $TABLE_RFIDS	= $TABLES->get_table_name('rfids');
my $TABLE_RES	= $TABLES->get_table_name('results');
my $TABLE_BOX 	= $TABLES->get_table_name('boxes');
my $TABLE_MEETINGS = $TABLES->get_table_name('meetings');

my $RFIDS;
my $BOXES;
my $BOX;

####################################
# PREAMBLE
####################################

print qq {
###############################################################           
# Resetting the database so that the meeting data can be      #
# recalculated starting with the 'meetings.pl' import script. #
###############################################################
};

####################################
# open db connection
$DBH = DBHandler->new()->connect();

##############################################################	           
# MAIN
##############################################################	 

print "=> resetting '$TABLE_MEETINGS' table ... \n";
$DBH->do( "TRUNCATE $TABLE_MEETINGS" )
		or die("Could not execute statement to reset $TABLE_MEETINGS: " . $DBH->errstr);	
		
print "=> resetting '$TABLE_RES' table ... \n";
$DBH->do( "UPDATE $TABLE_RES SET meetings='false'" )
		or die("Could not execute statement to reset $TABLE_RES: " . $DBH->errstr);

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