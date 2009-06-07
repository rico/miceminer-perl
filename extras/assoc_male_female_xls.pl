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
my $OUTFILE 	= $OUTDIR . 'assoc_male_female_.xls';

my $STEP		= 20;
my $DESCRIPTION = 'Data of the meetings between males and females';
my $TITLE		= 'Meetings between males and females';

##
# Define to which areas the boxes belong
my @A = (1 .. 10);
my @B = (11 .. 20);
my @C = (21 .. 30);
my @D = (31 .. 40);

##############################################################################
# PREAMBLE
##############################################################################
##
# Area hash setup
my $AREA_MEETINGS = {
	'A'=> {
			'boxes' => \@A,
			'data' => {}
		},
	'B'=> {	
			'boxes' => \@B,
			'data' => {}
		},
	'C'=> {
			'boxes' => \@C,	
			'data' => {}			
		},
	'D'=> {
			'boxes' => \@D,
			'data' => {}
		}
};

##
# Variables
my $BOXES;
my $OUTPUT = new IO::File(">".$OUTFILE);


####################
# Db connection
my $DBOBJ =  new DBHandler();
my $DBH = $DBOBJ->connect();

my $TABLE_MEETINGS = $DBOBJ->get_table_name('meetings');
my $TABLE_RFIDS = $DBOBJ->get_table_name('rfids');

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

##
# define columns name / field: key $MEETINGS_MALE_FEMALE / format for excel
print"column definitions ... \n";
my $col_num = -1;
my $COLUMNS = {	

	0 => {	
			'title' => 'male',
			'field' => 'male',
			'format' => 'str'
			},
	1 => {
	 		'title' => 'female',	
			'field' => 'female',
			'format' => 'str'
			},
	2 => {
	 		'title' => 'meeting count',	
			'field' => 'dt_freq',
			'format' => 'num'
			},
	3 => {
	 		'title' => 'meeting sum (sec)',	
			'field' => 'dt_sum',
			'format' => 'num'
			}			
};



##################################################
# data
##
# getting data from db

print"getting data from db ...\n";

my $meetings_male_female_sql = qq{
	SELECT id, TIME_TO_SEC(dt), SUM(TIME_TO_SEC(dt)) as dt_sum, COUNT(TIME_TO_SEC(dt)) as dt_freq, box, 
	rfid_from, (SELECT rfid.sex from rfid where rfid_from = rfid.id) as rfid_from_sex,
	rfid_to, (SELECT rfid.sex from rfid where rfid_to = rfid.id) as rfid_to_sex,
	typ
	FROM meetings 
	GROUP BY rfid_from, rfid_to, dt
	HAVING rfid_from_sex IS NOT NULL AND rfid_to_sex IS NOT NULL 
	AND rfid_from_sex != rfid_to_sex;
};

my $MEETINGS_MALE_FEMALE = $DBH->selectall_hashref($meetings_male_female_sql, 'id');

#print " => \$box_meetings: " . Dumper($MEETINGS_MALE_FEMALE) ."\n";
#exit;

#
				
##
# Looping through data and kind of sorting it by box
my $BOXES_DATA = ();

print"sorting data by box ...\n";
foreach my $meeting_id (keys %$MEETINGS_MALE_FEMALE) {

	my $values = $MEETINGS_MALE_FEMALE->{$meeting_id};
	my $box = $values->{'box'};
	
	
	##
	# check if the box is already a key with an anonymous hash as value
	# if true add the new key/values pair to the hash, if not create the 
	# anonymoous hash and add the first key/value pair
	if (defined $BOXES_DATA->{$box}) {
		$BOXES_DATA->{$box}->{$meeting_id} = $values;	
	} else {
		$BOXES_DATA->{$box} = {$meeting_id => $values};
	}
	
	

}

##
# Looping through box data and print a worksheet for each box
my $BOXES_MEETINGS;
print"print data for each box ...\n";
foreach my $box (sort { $a <=> $b } keys %$BOXES_DATA) {
		
	##
	# EXCEL
	my $row = 0;
	my $col = 0;
	my $box_ws = $XLS->add_worksheet("Box $box");
	
	# title
	$box_ws->write($row, $col, $TITLE . " for box $box", $title_format);
	$row += 2;
	# header
	
	##
	# Data
	my $box_meetings = $BOXES_DATA->{$box};
	my $box_meetings_reformat = {};

	##	
	# Figure out the area the box belongs to
	my $area = &find_area($box, $AREA_MEETINGS);
	if($area eq '') {
		print"BOX: $box AREA: $area\n";
	}
	#my $area = 'A';	
	
	#print"printing data for box: $box => area: $area\n";
	
	##
	# getting the rfid pairs 
	foreach my $meeting (keys %$box_meetings) {
		
		##
		# building new key by combining the rfids (first  part male second part female)
		my $values = $box_meetings->{$meeting};
		
		my $rfid_pair;
		my $male = 'from';
		if($values->{'rfid_from_sex'} eq 'm') {
			$rfid_pair = $values->{'rfid_from'} . $values->{'rfid_to'};
			$values->{'male'} = $values->{'rfid_from'};
			$values->{'female'} = $values->{'rfid_to'};
		} else {
			$rfid_pair = $values->{'rfid_to'} . $values->{'rfid_from'};
			$values->{'male'} = $values->{'rfid_to'};
			$values->{'female'} = $values->{'rfid_from'};
		}
		
		##
		# check if the rfid_pair is already a key with an anonymous hash as value
		# if true add the new key/values pair to the hash, if not create the 
		# anonymoous hash and add the first key/value pair
		if (defined $box_meetings_reformat->{$rfid_pair}) {
			$box_meetings_reformat->{$rfid_pair}->{'dt_freq'} += $box_meetings->{$meeting}->{'dt_freq'};	
			$box_meetings_reformat->{$rfid_pair}->{'dt_sum'} += $box_meetings->{$meeting}->{'dt_sum'};				
		} else {
			$box_meetings_reformat->{$rfid_pair} = $values ;
		}
		
		## 
		# Do the same with the hash holding all the data
		if (defined $BOXES_MEETINGS->{$rfid_pair}) {
			$BOXES_MEETINGS->{$rfid_pair}->{'dt_freq'} += $box_meetings->{$meeting}->{'dt_freq'};	
			$BOXES_MEETINGS->{$rfid_pair}->{'dt_sum'} += $box_meetings->{$meeting}->{'dt_sum'};				
		} else {
			$BOXES_MEETINGS->{$rfid_pair} = $values ;
			#print " => \$BOXES_MEETINGS->{$rfid_pair}: " . Dumper($BOXES_MEETINGS->{$rfid_pair}) ."\n";
		}
		
		## 
		# And again for the areas
		if (defined $AREA_MEETINGS->{$area}->{'data'}->{$rfid_pair}) {
			$AREA_MEETINGS->{$area}->{'data'}->{$rfid_pair}->{'dt_freq'} += $box_meetings->{$meeting}->{'dt_freq'};	
			$AREA_MEETINGS->{$area}->{'data'}->{$rfid_pair}->{'dt_sum'} += $box_meetings->{$meeting}->{'dt_sum'};				
		} else {
		
			$AREA_MEETINGS->{$area}->{'data'}->{$rfid_pair} = $values;
		}
	}
	
	&data_print_xls($box_ws, $box_meetings_reformat, $row);

}


##
# Printing worksheet for summary data
##
print"print data for all boxes boxes.\n";
# EXCEL
my ($row, $col) = 0;
my $sum_ws = $XLS->add_worksheet('All boxes');

# title
$sum_ws->write($row, $col, $TITLE . ' for all Boxes', $title_format);
$row += 2;

&data_print_xls($sum_ws, $BOXES_MEETINGS, $row);

##
# Printing worksheets for areas

print join(" area print\n", keys %$AREA_MEETINGS) . "\n"; 
#print " => \$AREA_MEETINGS: " . Dumper($AREA_MEETINGS) ."\n";
foreach my $area (keys %$AREA_MEETINGS) {
	
#	print"print data for area $area.\n";

	my ($row, $col) = 0;
	
	my $area_ws = $XLS->add_worksheet("Area " . $area);
	
	# title
	$sum_ws->write($row, $col, $TITLE . ' for area ' . $area, $title_format);
	$row += 2;
	
	my $area_data = $AREA_MEETINGS->{$area}->{'data'};
	print " => \$area_data: " . Dumper($area_data) ."\n";
	sleep(3);
	
	&data_print_xls($area_ws,$area_data, $row);

}
	

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
	
	##
	# print header
	foreach my $col (sort { $a <=> $b } keys %$COLUMNS) {
		$sheet->write_string($row, $col, $COLUMNS->{$col}->{'title'}, $header_format);
	}
	
	$row++;

	##
	# print sorted data
	my @sorted_keys = keys %$data;
	#print "data_print_xls => \$data: " . Dumper($data) ."\n";
	#print join("\n", @sorted_keys);
	if( $sorted_keys[0] =~ (/^\d+$/)) {
		@sorted_keys = sort { defined $a <=> $b  } keys %$data;
	} else {
		@sorted_keys = sort { defined $a cmp $b  } keys %$data;
	}
	
	foreach my $key (@sorted_keys) {
		
		my $values = $data->{$key};
		
		##
		# write row
		foreach my $col (sort { $a <=> $b } keys %$COLUMNS) {
		
			my $field = $COLUMNS->{$col}->{'field'};
			my $format = $COLUMNS->{$col}->{'format'};
			
			my $cell_val = $values->{$field}; 	#get value for this cell
			
			if( $format eq 'str') {
				$sheet->write_string($row, $col, $cell_val);
			} elsif ($format eq 'num' ) {
				$sheet->write_number($row, $col, $cell_val, $num_format);
			}
		}
		
		$row++;
	}
			
	return undef;
}

##
# Find out to which area the box belongs to
sub find_area {

	my ($box, $area_meetings) = @_;
	
	#print join(" area\n", keys %$area_meetings) ."\n";
	#print "find_area => \$area_meetings: " . Dumper($area_meetings) ."\n";
	#sleep(3);
	
	#print "find_area => \$AREA_MEETINGS: " . Dumper($AREA_MEETINGS) ."\n";
	
	foreach my $temp_area (keys %$area_meetings) {
	
		#print"temp Area: $temp_area\n";
		
		my $boxes_for_area = $area_meetings->{$temp_area}->{'boxes'};
		
		#print "Boxes for $temp_area: " . join("\t", @$boxes_for_area) . "\n";
		
	 	my %is_area = ();
	 	
    	for (@$boxes_for_area) { $is_area{sprintf("%02d",$_)} = 1 };
    	
    	if (defined $is_area{$box}) { 
    		return $temp_area;
    	}
		
	}	
	

}
__END__
