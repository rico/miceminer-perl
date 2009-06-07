#!/usr/bin/perl -w

##############################################################################
# Create .dbf files for the rfids. 
#
# rleuthold@access.ch - 30.3.2009
##############################################################################

use strict;
use DBI;

use lib 'lib';
use lib::DBHandler;
use lib::XMLPaths;
use lib::DBTables;
use XBase;

use Data::Dumper;

##
# Global variables
my $DBH; 
my $TABLES		= DBTables->new();
my $PATHS		= XMLPaths->new();
my $PERLCONFIG	= PerlConfig->new();

# Paths / directories
my $DATA_PATH 	= $PATHS->get_path('data');
my $DBASE_PATH 	= $DATA_PATH ."dbase/";

# database tables
my $TABLE_RFIDS	= $TABLES->get_table_name('rfids');
my $TABLE_DATA	= $TABLES->get_table_name('data');
my $TABLE_DBASE = 'dbase';


####################################
# PREAMBLE
####################################


####################################
# open db connection
$DBH = DBHandler->new()->connect();

####################################
# MAIN
####################################

# The years and the months in the data table
my $YEAR_MONTH_DATA = $DBH->selectall_arrayref(qq{select distinct( YEAR(time)) as year, MONTH(time) as month  FROM $TABLE_DATA}, ) 
	or die( "Could not get years and months from $TABLE_DATA: " . $DBH->errstr);

my $YEAR_MONTH_HASH = {}; 
foreach my $year_month (@$YEAR_MONTH_DATA) {
	
	$YEAR_MONTH_HASH->{ @$year_month[0] }->{ @$year_month[1] } = 1;
}
	
print "\$YEAR_MONTH_DATA => " . Dumper($YEAR_MONTH_HASH) ."\n";

# The years and the months in the dbase table
my $YEAR_MONTH_DBASE = $DBH->selectall_arrayref(qq{SELECT year, month  FROM $TABLE_DBASE}) 
	or die( "Could not get years and months from $TABLE_DBASE: " . $DBH->errstr, , { Slice => {} });
 
foreach my $year_month (@$YEAR_MONTH_DBASE) {
    delete $YEAR_MONTH_HASH->{ @$year_month[0] }->{ @$year_month[1] };    
};

print "\$YEAR_MONTH_DATA => " . Dumper($YEAR_MONTH_HASH) ."\n";
exit;
	
##
# 