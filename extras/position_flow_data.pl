#!/usr/bin/perl -w

##############################################################################
# Writes in/out data for a specified period to a file called network_flow.txt
# located in the directory set in the CONFIGURATION section.
#
# The data is taken from the result table.
#
# rleuthold@access.ch // 25.3.2009
##############################################################################
use strict;


use DBI;
use Data::Dumper;
use Fcntl;

use lib 'lib';
use lib::DBHandler;

##############################################################################
# CONFIGURATION
##############################################################################
my $TABLE_RES	= "res";
my $TABLE_BOX	= "box";
my $DATA_PATH 	= "/Users/rico/Desktop/";
my $START		= '2008-06-01';
my $END			= '2008-08-31';
##############################################################################

print "\n--------------------------------------------------\n";
print "START position_flow_data.pl\n";
print "From: $START\tTo: $END\n";
print "--------------------------------------------------\n\n";

#############################################################################
# open db connection
my $DBH = DBHandler->new()->connect();
#############################################################################


##
# setting up file for output
my $ALL_IN_ONE_FILE	= $DATA_PATH."network_flow.txt";

##
# opening file for writing
open(RES,"> $ALL_IN_ONE_FILE")  or die("Can't open file $ALL_IN_ONE_FILE: $!");
print RES "time                rfid        box direction\n";
print RES "-------------------+-----------+---+----------\n";



##############################################################################
# MAIN
##############################################################################	

print "Getting Data ... \n";

my $DATA = $DBH->selectall_hashref(
	qq{	select id ,rfid, box_in, box_out, UNIX_TIMESTAMP(box_in) AS `box_in_ut`, UNIX_TIMESTAMP(box_out) AS `box_out_ut`, box 
		FROM $TABLE_RES where DATE(box_in) > ('$START') AND DATE(box_out) < ('$END')},
	'id') 
	or die("OUT data error: " . $DBH->errstr);					
	
my @IN_OUT_SPLITTED = ();

print "Data: " . scalar(keys(%$DATA)) . "\n";
sleep(3);

foreach my $data_key( keys(%$DATA)) {	

	my $in_data = {
		'time'		=> $DATA->{ $data_key }->{'box_in'},
		'ut_time'	=> $DATA->{ $data_key }->{'box_in_ut'},		
		'rfid'		=> $DATA->{ $data_key }->{'rfid'},
		'box' 		=> $DATA->{ $data_key }->{'box'},
		'dir'		=> 'in'
	};
	
	push(@IN_OUT_SPLITTED, $in_data);
	
	my $out_data = {
		'time'		=> $DATA->{ $data_key }->{'box_out'},
		'ut_time'	=>  $DATA->{ $data_key }->{'box_out_ut'},
		'rfid'		=> $DATA->{ $data_key }->{'rfid'},
		'box' 		=> $DATA->{ $data_key }->{'box'},
		'dir'		=> 'out'
	}; 
	
	push(@IN_OUT_SPLITTED, $out_data);
	
}

#print " => \$IN_OUT_SPLITTED: " . Dumper(\@IN_OUT_SPLITTED) ."\n";
#exit;
my $DATACOUNT = 0;

foreach ( sort{ $a->{'ut_time'} <=> $b->{'ut_time'} } (@IN_OUT_SPLITTED) ) {

	my $dataset = $_;	
	##
	# print data
	print RES $dataset->{'time'} . "\t" . $dataset->{'rfid'} . "\t" . $dataset->{'box'} . "\t" . $dataset->{'dir'} . "\n";
	
	# inform user
	(($DATACOUNT % 100) == 0) ? print "\n[$DATACOUNT]\t" : print "."; 
	
	$DATACOUNT++;
	
}

print(" [$DATACOUNT]\n");

close(RES);
print "\n-------------------------\n";
print "FINISHED position_flow_data.pl\n";
print "-------------------------\n\n";
$DBH->disconnect();

# my @args = ( "bbedit $ALL_IN_ONE_FILE");
# system(@args) == 0
# 	or die "system @args failed: $?";


$DBH->disconnect();