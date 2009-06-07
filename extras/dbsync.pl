#!/usr/bin/perl -w

use strict;
use Data::Dumper;


##############################################################################
# SET THIS !!!!!!!!!!!!!
##############################################################################

##
# tables to sync
my @tables 		= ( qw (ant ant_count box box_count data dir logs res rfid rfid_count));

##
# localhost mysql information
my $localUser 	= "importer";
my $localPass	= "imP*maIce";
my $localHost	= "localhost";
my $localDB		= "micedata";

##
# remote host mysql information
my $remUser		= "mice";
my $remPass		= "MkmNm*07";
my $remHost		= "mysql.uzh.ch";
my $remDB		= "mice";


my $BASE_PATH	= "/var/www/mouse/";
my $DATA_PATH 	= $BASE_PATH. "data/";
my $SCRIPT_PATH	= $BASE_PATH. "perl/"; 
##############################################################################



my $localPart 	= "$localUser:$localPass\@$localHost/$localDB.";
my $remotePart 	= "$remUser:$remPass\@$remHost/$remDB.";

########################################################################
# open local db connection
# my $DBH = DBI->connect('DBI:mysql:$localDB;mysql_socket=/var/mysql/mysql.sock', $localUser, $localPass,
# 					{ RaiseError => 0, AutoCommit => 0 })
# 	           or die "Could not connect to database: $DBI::errstr";
# 	           
# my $LOCAL_TABLES = $DBH->selectall_arrayref("SHOW TABLES")
# 		or die("Could not execute statement to show tables: " . $DBH->errstr);
	
print"\nbsync.pl: sync tables to remote host [$remHost]\n";


foreach my $table (@tables) {
	print"Syncing table [$table] ... \n";
	my @args = ("mysql-table-sync", "--execute", "--verbose", "-a", "topdown", $localPart.$table, $remotePart.$table );
	#print join(" ", @args) . "\n";
	
	system(@args) == 0
 	 or die "system: syncing table [$table] failed: $?";
}

print"\ndbsync.pl: tables synced.\n";


my @args = ( $SCRIPT_PATH."counter.pl");
system(@args) == 0
	or die "system @args failed: $?";
exit;



#mysql-table-sync --execute --verbose -a topdown importer:imP*maIce@localhost/micedata.data mice:MkmNm*07@mysql.uzh.ch/mice.data