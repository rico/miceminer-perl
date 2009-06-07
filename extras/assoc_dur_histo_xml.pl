#!/usr/bin/perl

##############################################################################
# This scripts generates an xml with the frequency of the meeting
# duration (duration histogram)
#
# rleuthold@access.ch // 28.3.2008
##############################################################################
use strict;
use warnings;
use lib 'lib';

use Data::Dumper;
use DBHandler;
use XML::Writer;
use IO qw(File);
use POSIX qw(ceil);

my $TIMESTAMP = time() * 1000;

##############################################################################
# SET THIS
##############################################################################
my $BASE_PATH 	= "/Library/WebServer/mice/";
my $OUTDIR 		= $BASE_PATH . 'xml/';
my $OUTFILE 	= $OUTDIR . 'assoc_dur_histo_'. $TIMESTAMP . '_.xml';

my $STEP		= 5;
my $DESCRIPTION = 'Frequency data of the meetings for discrete time steps';
my $XTITLE		= 'time steps (interval' . $STEP . 'sec.)';
my $YTITLE		= 'count';

##############################################################################
# PREAMBLE
##############################################################################
##
# Variables
my $BOXES;
my $OUTPUT = new IO::File(">".$OUTFILE);


####################
# Db connection
my $DBOBJ =  new DBHandler();
my $DBH = $DBOBJ->connect();

my $TABLE_MEETINGS = $DBOBJ->get_table_name('meetings');

##############################################################################
# MAIN
##############################################################################
##
# Setting up the xml
my $WRITER = new XML::Writer( 	OUTPUT 		=> $OUTPUT,
								NEWLINES 	=> 1,
								ENCODING 	=> 'utf-8',
								DATA_MODE 	=> 1,
								DATA_INDENT => 1
							);

$WRITER->xmlDecl();
#$WRITER->doctype( 'XML' );

$WRITER->startTag( 'chart' ); # root tag
##################################################
# info
$WRITER->startTag( 'info' );

$WRITER->dataElement( 'desc', $DESCRIPTION );
$WRITER->dataElement( 'gendate', $TIMESTAMP );

##
# getting date range values
my ($START, $END) =  $DBH->selectrow_array("SELECT MIN(`from`), MAX(`to`) FROM $TABLE_MEETINGS");

$WRITER->startTag( 'daterange' );
$WRITER->dataElement( 'start', $START );
$WRITER->dataElement( 'end', $END );
$WRITER->endTag();

##
# Axis titles
$WRITER->dataElement( 'xtitle', $XTITLE );
$WRITER->dataElement( 'ytitle', $YTITLE );

$WRITER->endTag();

# END info
##################################################

##################################################
# data
$WRITER->startTag( 'data' );
$WRITER->dataElement( 'start', $START );
$WRITER->dataElement( 'end', $END );

##
# Getting the boxes
$BOXES = $DBH->selectcol_arrayref("SELECT DISTINCT box FROM $TABLE_MEETINGS");
my @BOXES_SORTED = sort {$a <=> $b} @$BOXES;

##
# Getting min max values of meeting duration
#my ($MIN_DUR, $MAX_DUR) =  $DBH->selectrow_array("SELECT MIN(TIME_TO_SEC(dt)), MAX(TIME_TO_SEC(dt)) FROM $TABLE_MEETINGS");



##
# Looping through boxes
foreach my $BOX (@BOXES_SORTED) {

	$WRITER->startTag( 'group', 'desc' 	=> 'box',
								'id'	=> $BOX
					);
	##
	# Getting distinct meeting durations and the corresponding frequencies
	my $dist_dt_meetings_sql = "SELECT TIME_TO_SEC(dt) as dt_sec, COUNT(TIME_TO_SEC(dt)) AS freq_count FROM $TABLE_MEETINGS  WHERE BOX = $BOX GROUP BY dt ";
	my $box_meetings = $DBH->selectall_hashref($dist_dt_meetings_sql, 'dt_sec');
	
	#print " => \$box_meetings: " . Dumper($box_meetings) ."\n";
	#print keys(%$box_meetings) . "\n";
	
	##
	# sorting meetings based on duration
	my %meetings_ranged = ();
	
	foreach my $dt (keys %$box_meetings) {
	
		my $range = ceil($dt / $STEP) * 5;
		#print "$dt / $STEP = $range\n";
	
		$meetings_ranged{$range} += $box_meetings->{$dt}->{'freq_count'};
	}

	print " => \$meetings_ranged: " . Dumper(\%meetings_ranged) ."\n";
	print"$BOX\n";


					
	$WRITER->dataElement( 'xval',  );
	$WRITER->dataElement( 'yval',  );				
	
	$WRITER->endTag();
	
	exit;
}

$WRITER->endTag();
# END data
##################################################

##
# finishing xml
$WRITER->endTag(  );
$WRITER->end(  );
$DBH->disconnect();
