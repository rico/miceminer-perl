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
use Spreadsheet::WriteExcel;
use IO qw(File);
use POSIX qw(ceil);

my $TIMESTAMP = time() * 1000;

##############################################################################
# SET THIS
##############################################################################
my $BASE_PATH 	= "/Library/WebServer/mice/";
my $OUTDIR 		= $BASE_PATH . 'xls/';
my $OUTFILE 	= $OUTDIR . 'assoc_dur_histo_'. $TIMESTAMP . '_.xls';

my $STEP		= 20;
my $DESCRIPTION = 'Frequency data of the meetings for discrete time steps';
my $TITLE		= 'Frequency count for meeting durations on discrete timesteps';
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
# Setting up the xls
my $XLS = Spreadsheet::WriteExcel->new($OUTFILE)
		or die("Couldn't create workbook $!");
		
##
# xls formats
my $title_format = $XLS->add_format(
					font  => 'Arial',
                    size  => 14,
                    color => 'black',
                    bold  => 1,
                    pattern  => 0,
					border => 0);				
					
my $header_format = $XLS->add_format(
					font  => 'Arial',
                    size  => 12,
                    color => 'white',
                    bold  => 1,
					bg_color => 'grey',
                    pattern  => 0,
					border => 1
				);
				
my $string_format 	= $XLS->add_format(num_format => '@');
my $num_format		= $XLS->add_format();
$num_format->set_num_format('0');

my $header_row = ['time range','count','time sum'];

#write_string($row, $column, $string, $format)
#write_number($row, $column, $number, $format)

##################################################
# info

##
# getting date range values
my ($START, $END) =  $DBH->selectrow_array("SELECT MIN(`from`), MAX(`to`) FROM $TABLE_MEETINGS");


# END info
##################################################

##################################################
# data

##
# Data for all boxes
my %DATA = ();

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
	
	print"Data for box: $BOX\n";
	
	##
	# EXCEL
	my $row = 0;
	my $col = 0;
	my $box_ws = $XLS->add_worksheet("Box $BOX");
	
	# title
	$box_ws->write($row, $col, $TITLE . " for box $BOX", $title_format);
	$row += 2;
	# header
	
	##
	# Data
	my %box_data = ();
	

	##
	# Getting distinct meeting durations and the corresponding frequencies
	my $dist_dt_meetings_sql = "SELECT TIME_TO_SEC(dt) as dt_sec, COUNT(TIME_TO_SEC(dt)) AS freq_count FROM $TABLE_MEETINGS  WHERE BOX = $BOX GROUP BY dt ";
	my $box_meetings = $DBH->selectall_hashref($dist_dt_meetings_sql, 'dt_sec');
	
	#print " => \$box_meetings: " . Dumper($box_meetings) ."\n";
	#print keys(%$box_meetings) . "\n";
	#exit;
	
	##
	# sorting meetings based on duration
	my %meetings_ranged = ();
	
	foreach my $dt (keys %$box_meetings) {
	
		my $range = ceil($dt / $STEP) * $STEP;
		#print "$dt / $STEP = $range\n";
	
		# Box level data
		
		$meetings_ranged{$range}{'count'} += $box_meetings->{$dt}->{'freq_count'};
		$meetings_ranged{$range}{'sum'} += ($dt * $box_meetings->{$dt}->{'freq_count'});
		
		# all Data
		$DATA{$range}{'count'} += $box_meetings->{$dt}->{'freq_count'};
		$DATA{$range}{'sum'} = ($dt * $box_meetings->{$dt}->{'freq_count'});
	}
	
	
	&data_print_xls($box_ws, \%meetings_ranged, $row);


	#print " => \$meetings_ranged: " . Dumper(\%meetings_ranged) ."\n";
	#print"$BOX\n";

}


##
# Printing worksheet for summary data
##
print"Summary data\n";

# EXCEL
my ($row, $col) = 0;
my $sum_ws = $XLS->add_worksheet('All boxes');

# title
$sum_ws->write($row, $col, $TITLE . ' for all Boxes', $title_format);
$row += 2;

&data_print_xls($sum_ws, \%DATA, $row);
	

# END data
##################################################

##
# finishing xls
$XLS->close();
$DBH->disconnect();
print"\n-------------------\n";
print"END";
print"\n-------------------\n";

# open the file
my @ARGS = ("open $OUTFILE");
	system(@ARGS) == 0
		or die "system @ARGS failed: $?";

##############################################################################
# SUBS
##############################################################################

	
##
# print data (hash) to excel worhsheet
sub data_print_xls {

	my ($sheet, $data, $row) = @_;
	my $col = 0;
	
	##
	# print header
	$sheet->write_row($row, $col, $header_row, $header_format);
	$row++;
	
	##
	# print sorted data
	
	foreach my $range (sort { $a <=> $b } keys %$data) {
		$col = 0;
		
		$sheet->write_string($row, $col, $range - $STEP + 1 . '-' . $range);
		$col++;

		$sheet->write_number($row, $col, $data->{$range}->{'count'}, $num_format);
		$col++;
		
		$sheet->write_number($row, $col, $data->{$range}->{'sum'}, $num_format);
		$row++;
		
		
	}
		
	return undef;

}	

