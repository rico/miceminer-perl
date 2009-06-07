#!/usr/bin/perl -w

##############################################################################
# Calls the counter.pl script to calculate for a specific day.
#
# rleuthold@access.ch // 8.3.2009
##############################################################################
use strict;
use CGI;
use DBI;

use lib 'lib';
use lib::DBHandler;
use lib::DBTables;
use XML::Writer;
use Data::Dumper;
use File::Copy;

my $DBH; 
my $TABLES = DBTables->new();
my $PERLCONFIG	= PerlConfig->new();
my $DAYS_TO_COUNT_TABLE = $TABLES->get_days_to_count_table();
my $SCRIPT_PATH	= $PERLCONFIG->get_scriptsfolder();

##
# CGI object
my $CGI = new CGI();

##
# header
print $CGI->header('text/xml');

##
# xml feedback
my $FEEDBACK = '';
my $XMLWRITER = new XML::Writer(OUTPUT => $FEEDBACK, NEWLINES => 1);
$XMLWRITER->startTag("feedback");

##
# getting parameters
# 
my $DAY = $CGI->param('day') || &Exit("day missing");

if($DAY !~ /\d{4}-\d{2}-\d{2}/) {
	&Exit("Day has wrong format (yyyy-mm-dd): $DAY ");
}


##############################################################################
# PREAMBLE
##############################################################################

#############################################################################
# open db connection
$DBH = DBHandler->new()->connect();
#############################################################################
##
# Create the 'temporary' table where the counter.pl script will read out the days to count.
$DBH->do(qq{CREATE TABLE IF NOT EXISTS `$DAYS_TO_COUNT_TABLE` (`day` date NOT NULL, PRIMARY KEY (`day`) )}) 
	|| &Exit("Could not create table '$DAYS_TO_COUNT_TABLE': " . $DBH->errstr);
	
##
# insert the day to count in that table
$DBH->do(qq{INSERT IGNORE INTO $DAYS_TO_COUNT_TABLE (`day`) VALUES('$DAY')})
	|| &Exit("Could not execute statement to insert day into $DAYS_TO_COUNT_TABLE for $DAY: " . $DBH->errstr);	
		


##
# Count for that day
my @args = ( "/usr/bin/perl -I$SCRIPT_PATH " . $SCRIPT_PATH."counter.pl --quiet");
system(@args) == 0 	
	or &Exit("system @args failed: $?");

##
# give feedback
$XMLWRITER->dataElement("success","Updated successfully", 'id' => $DAY);
&Exit();

#########################################################
# SUBS
#########################################################

sub Exit {
	
	my $error = '';
	$error = shift;
	
	if( defined($DBH) ) {
		$DBH->disconnect();
	}
	
	
	if( defined($error) ) {
		$XMLWRITER->dataElement("error",$error);
	}
	
	##
	# feedback
	$XMLWRITER->endTag("feedback");
	$XMLWRITER->end();
	
	print $FEEDBACK;
	
	exit;
}