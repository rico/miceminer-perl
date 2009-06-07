#!/usr/bin/perl -w

use strict;
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
my $DATA_PATH 	= $PATHS->get_path('data');
my $IMPORTED_FOLDER 	= $PATHS->get_path('imported');
my $IMPORTED_LOGS_FOLDER = $PATHS->get_path('importedlogs');

my $SCRIPT_PATH	= $PERLCONFIG->get_scriptsfolder();
my $DB			= $PERLCONFIG->get_dbname();

# database tables
my $TABLE_ANTENNAS 	= $TABLES->get_table_name('antennas'); 	# table antennas
my $TABLE_DATA	= $TABLES->get_table_name('data');
my $TABLE_LOGS	= $TABLES->get_table_name('logfiles');
my $TABLE_RFIDS	= $TABLES->get_table_name('rfids');

my $DAYS_TO_COUNT_TABLE = $TABLES->get_days_to_count_table();

my $BACKUPUSER	= $PERLCONFIG->get_dbbackupuser();
my $DBBACKUPDIR	= $PERLCONFIG->get_dbbackupdirectory();

my $DATEFORMAT	= "20%02d-%02d-%02d %02d:%02d:%02d";
my $DAY_FOLDER;



#############################################################################
# open db connection
$DBH = DBHandler->new()->connect();
#############################################################################

print "Perlconfig:\n";
my $xml = $PERLCONFIG->get_config_xml_path();
print "XML:" . $xml . "\n";

#print "XMLPATHS:\n";
#
#$PATHS->dump_paths();

print "DBH:\n";
my $ANTENNAS = $DBH->selectall_hashref("SELECT id FROM $TABLE_ANTENNAS",'id');
print "\$ANTENNAS => " . Dumper($ANTENNAS) ."\n";

$DBH->disconnect();

